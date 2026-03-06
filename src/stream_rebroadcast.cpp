#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <chrono>
#include <fstream>
#include <sstream>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "spdlog/spdlog.h"

#include "stream_rebroadcast.h"

extern "C" {
#include "osd.h"
}

std::atomic<int> rebroadcast_enabled{0};

// SAP announcement interval in seconds
static const int SAP_INTERVAL_SEC = 5;

// Maximum number of frames allowed in the RPC queue.  When the queue exceeds
// this depth, older FRAME entries are dropped.  This prevents unbounded queue
// growth when the rebroadcast thread momentarily falls behind the receiver,
// which would otherwise cause a burst of frames with compressed timestamps
// that floods the remote receiver's jitter buffer.
static const size_t MAX_FRAME_QUEUE = 10;

// Check if byte-stream data contains a keyframe.
// For H.265: IDR_W_RADL (19), IDR_N_LP (20), CRA_NUT (21), BLA types (16-18)
// For H.264: IDR slice (5)
static bool frame_is_keyframe(const uint8_t* data, size_t size, VideoCodec codec) {
    if (!data || size < 5) return false;
    for (size_t i = 0; i + 3 < size; ) {
        if (data[i] == 0x00 && data[i + 1] == 0x00) {
            size_t sc_len = 0;
            if (data[i + 2] == 0x01) {
                sc_len = 3;
            } else if (data[i + 2] == 0x00 && i + 3 < size && data[i + 3] == 0x01) {
                sc_len = 4;
            }
            if (sc_len > 0 && (i + sc_len) < size) {
                uint8_t nal_byte = data[i + sc_len];
                if (codec == VideoCodec::H265) {
                    uint8_t nal_type = (nal_byte >> 1) & 0x3f;
                    if (nal_type >= 16 && nal_type <= 21) return true;
                } else if (codec == VideoCodec::H264) {
                    uint8_t nal_type = nal_byte & 0x1f;
                    if (nal_type == 5) return true;
                }
                i += sc_len + 1;
                continue;
            }
        }
        i++;
    }
    return false;
}

StreamRebroadcast::StreamRebroadcast(rebroadcast_params params) {
    host_ = params.host;
    port_ = params.port;
    codec_ = params.codec;
    out_codec_ = params.codec;
    bitrate_ = params.bitrate;
    transcode_ = params.transcode;
    pipeline_ = NULL;
    appsrc_ = NULL;
    payloader_ = NULL;
    encoder_ = NULL;
    sap_fd_ = -1;
    local_ip_ = 0;
    sap_msg_id_ = 0;
    sprop_updated_ = false;
    running_ = false;
    idr_seen_ = false;
    frame_count_ = 0;
    srand(time(NULL) ^ getpid());
    session_id_ = (uint32_t)rand();
}

StreamRebroadcast::~StreamRebroadcast() {
    destroy_pipeline();
    if (sap_fd_ >= 0) {
        close_sap_socket();
    }
}

void StreamRebroadcast::frame(std::shared_ptr<std::vector<uint8_t>> frame) {
    rebroadcast_rpc rpc = {
        .command = rebroadcast_rpc::RPC_FRAME,
        .frame = frame
    };
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // Drop oldest entries when the queue grows too deep.  Old frames
        // would arrive with compressed timestamps once processed, flooding
        // the receiver's jitter buffer and causing packet loss.
        while (queue_.size() > MAX_FRAME_QUEUE) {
            queue_.pop();
        }
        queue_.push(rpc);
    }
    cv_.notify_one();
}

void StreamRebroadcast::start() {
    rebroadcast_rpc rpc = {
        .command = rebroadcast_rpc::RPC_START
    };
    enqueue(rpc);
}

void StreamRebroadcast::stop() {
    rebroadcast_rpc rpc = {
        .command = rebroadcast_rpc::RPC_STOP
    };
    enqueue(rpc);
}

void StreamRebroadcast::toggle() {
    rebroadcast_rpc rpc = {
        .command = rebroadcast_rpc::RPC_TOGGLE
    };
    enqueue(rpc);
}

void StreamRebroadcast::set_bitrate(int bps) {
    bitrate_ = bps;
    rebroadcast_rpc rpc = {
        .command = rebroadcast_rpc::RPC_SET_BITRATE
    };
    enqueue(rpc);
}

void StreamRebroadcast::shutdown() {
    rebroadcast_rpc rpc = {
        .command = rebroadcast_rpc::RPC_SHUTDOWN
    };
    enqueue(rpc);
}

void StreamRebroadcast::enqueue(rebroadcast_rpc rpc) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        queue_.push(rpc);
    }
    cv_.notify_one();
}

void *StreamRebroadcast::__THREAD__(void *param) {
    pthread_setname_np(pthread_self(), "__REBROADCAST");
    ((StreamRebroadcast *)param)->loop();
    return nullptr;
}

// GStreamer bus handler for error/warning/state-change logging
GstBusSyncReply StreamRebroadcast::bus_sync_handler(GstBus *bus, GstMessage *msg, gpointer data) {
    switch (GST_MESSAGE_TYPE(msg)) {
    case GST_MESSAGE_ERROR: {
        GError *err = NULL;
        gchar *dbg = NULL;
        gst_message_parse_error(msg, &err, &dbg);
        spdlog::error("Rebroadcast pipeline error: {} ({})", err->message, dbg ? dbg : "");
        g_error_free(err);
        g_free(dbg);
        break;
    }
    case GST_MESSAGE_WARNING: {
        GError *err = NULL;
        gchar *dbg = NULL;
        gst_message_parse_warning(msg, &err, &dbg);
        spdlog::warn("Rebroadcast pipeline warning: {} ({})", err->message, dbg ? dbg : "");
        g_error_free(err);
        g_free(dbg);
        break;
    }
    case GST_MESSAGE_STATE_CHANGED: {
        if (GST_MESSAGE_SRC(msg) == GST_OBJECT(static_cast<StreamRebroadcast *>(data)->pipeline_)) {
            GstState old_st, new_st, pending;
            gst_message_parse_state_changed(msg, &old_st, &new_st, &pending);
            spdlog::info("Rebroadcast pipeline state: {} -> {} (pending {})",
                         gst_element_state_get_name(old_st),
                         gst_element_state_get_name(new_st),
                         gst_element_state_get_name(pending));
        }
        break;
    }
    default:
        break;
    }
    return GST_BUS_PASS;
}

// Called from GStreamer thread when payloader negotiates caps with sprop parameters
void StreamRebroadcast::on_caps_change(GstPad *pad, GParamSpec *pspec, gpointer data) {
    StreamRebroadcast *self = static_cast<StreamRebroadcast *>(data);
    GstCaps *caps = gst_pad_get_current_caps(pad);
    if (!caps) return;

    GstStructure *s = gst_caps_get_structure(caps, 0);
    const gchar *encoding = gst_structure_get_string(s, "encoding-name");

    std::lock_guard<std::mutex> lock(self->sprop_mtx_);

    if (encoding && g_str_equal(encoding, "H265")) {
        const gchar *v = gst_structure_get_string(s, "sprop-vps");
        const gchar *sp = gst_structure_get_string(s, "sprop-sps");
        const gchar *pp = gst_structure_get_string(s, "sprop-pps");
        if (v) self->sprop_vps_ = v;
        if (sp) self->sprop_sps_ = sp;
        if (pp) self->sprop_pps_ = pp;
        self->sprop_updated_ = true;
        spdlog::info("Rebroadcast: got H.265 codec parameters from payloader");
    } else if (encoding && g_str_equal(encoding, "H264")) {
        const gchar *sprops = gst_structure_get_string(s, "sprop-parameter-sets");
        if (sprops) {
            std::string sp(sprops);
            size_t comma = sp.find(',');
            if (comma != std::string::npos) {
                self->sprop_sps_ = sp.substr(0, comma);
                self->sprop_pps_ = sp.substr(comma + 1);
            } else {
                self->sprop_sps_ = sp;
            }
            self->sprop_updated_ = true;
            spdlog::info("Rebroadcast: got H.264 codec parameters from payloader");
        }
    }

    gst_caps_unref(caps);
}

int StreamRebroadcast::build_pipeline() {
    std::string caps_media = (codec_ == VideoCodec::H264) ? "h264" : "h265";
    std::string parser = (codec_ == VideoCodec::H264) ? "h264parse" : "h265parse";

    // Build pipeline string.  appsrc properties are set programmatically
    // below for reliability (enum values like format/stream-type).
    // A leaky queue sits between appsrc and the parser: when the pipeline
    // can't consume data fast enough, old buffers are dropped (leaky=2
    // means "downstream", i.e. oldest buffers are discarded first).
    // This prevents stale data accumulation inside GStreamer that would
    // otherwise cause a burst of RTP packets with compressed timestamps.
    std::stringstream ss;
    ss << "appsrc name=src ! ";
    ss << "queue max-size-buffers=3 max-size-time=0 max-size-bytes=0 leaky=downstream ! ";
    ss << parser << " config-interval=-1 ! ";

    if (transcode_) {
        // Transcode: decode with MPP hardware, re-encode as H.265
        ss << "mppvideodec ! ";
        ss << "mpph265enc name=enc ! ";
        ss << "h265parse config-interval=-1 ! ";
        ss << "rtph265pay config-interval=-1 pt=96 mtu=1400 name=pay ! ";
        out_codec_ = VideoCodec::H265;
    } else {
        // Passthrough: just re-packetize with proper RFC-compliant RTP
        std::string pay = (codec_ == VideoCodec::H264) ? "rtph264pay" : "rtph265pay";
        ss << pay << " config-interval=-1 pt=96 mtu=1400 name=pay ! ";
        out_codec_ = codec_;
    }

    ss << "udpsink host=" << host_ << " port=" << port_;

    in_addr_t host_addr = inet_addr(host_);
    if (host_addr != INADDR_NONE) {
        uint32_t addr = ntohl(host_addr);
        bool is_multicast = ((addr & 0xF0000000) == 0xE0000000);
        if (is_multicast) {
            ss << " auto-multicast=true ttl=2";
        }
    }
    ss << " sync=false";

    std::string pipeline_str = ss.str();
    spdlog::info("Rebroadcast: pipeline: {}", pipeline_str);

    GError *error = NULL;
    pipeline_ = gst_parse_launch(pipeline_str.c_str(), &error);
    if (!pipeline_) {
        spdlog::error("Rebroadcast: failed to create pipeline: {}",
                      error ? error->message : "unknown error");
        if (error) g_error_free(error);
        if (transcode_) {
            spdlog::warn("Rebroadcast: transcode not available, falling back to passthrough");
            transcode_ = false;
            return build_pipeline();
        }
        return -1;
    }
    if (error) {
        spdlog::warn("Rebroadcast: pipeline warning: {}", error->message);
        g_error_free(error);
    }

    appsrc_ = gst_bin_get_by_name(GST_BIN(pipeline_), "src");
    payloader_ = gst_bin_get_by_name(GST_BIN(pipeline_), "pay");

    if (!appsrc_ || !payloader_) {
        spdlog::error("Rebroadcast: failed to get pipeline elements");
        destroy_pipeline();
        return -1;
    }

    // Configure appsrc for live streaming.
    // block=FALSE is critical: without it push_buffer() blocks when the
    // internal queue is full (default 200 KB).  That stalls the entire
    // rebroadcast thread because h265parse cannot produce output until
    // it sees VPS/SPS/PPS in a keyframe, so data backs up in the queue.
    g_object_set(appsrc_,
        "is-live",       TRUE,
        "do-timestamp",  TRUE,
        "format",        GST_FORMAT_TIME,
        "stream-type",   GST_APP_STREAM_TYPE_STREAM,
        "block",         FALSE,
        "max-bytes",     (guint64)0,  /* unlimited queue */
        NULL);

    // Set input caps on appsrc
    GstCaps *src_caps = gst_caps_new_simple(
        (std::string("video/x-") + caps_media).c_str(),
        "stream-format", G_TYPE_STRING, "byte-stream",
        NULL);
    g_object_set(appsrc_, "caps", src_caps, NULL);
    gst_caps_unref(src_caps);

    // Set encoder bitrate if transcoding
    if (transcode_) {
        encoder_ = gst_bin_get_by_name(GST_BIN(pipeline_), "enc");
        if (encoder_ && bitrate_ > 0) {
            g_object_set(encoder_, "bps", bitrate_, NULL);
            spdlog::info("Rebroadcast: encoder bitrate: {} bps", bitrate_);
        }
    }

    // Listen for caps changes on payloader src pad (to get sprop parameters)
    GstPad *pay_src = gst_element_get_static_pad(payloader_, "src");
    if (pay_src) {
        g_signal_connect(pay_src, "notify::caps", G_CALLBACK(on_caps_change), this);
        gst_object_unref(pay_src);
    }

    // Set up bus error handler
    GstBus *bus = gst_element_get_bus(pipeline_);
    gst_bus_set_sync_handler(bus, bus_sync_handler, this, NULL);
    gst_object_unref(bus);

    // Start the pipeline
    GstStateChangeReturn ret = gst_element_set_state(pipeline_, GST_STATE_PLAYING);
    if (ret == GST_STATE_CHANGE_FAILURE) {
        spdlog::error("Rebroadcast: failed to start pipeline");
        destroy_pipeline();
        if (transcode_) {
            spdlog::warn("Rebroadcast: transcode pipeline failed, trying passthrough");
            transcode_ = false;
            return build_pipeline();
        }
        return -1;
    }

    frame_count_ = 0;
    idr_seen_ = false;

    spdlog::info("Rebroadcast: streaming to {}:{} ({}, {} -> {})",
                 host_, port_,
                 transcode_ ? "transcode" : "passthrough",
                 (codec_ == VideoCodec::H264) ? "H264" : "H265",
                 (out_codec_ == VideoCodec::H264) ? "H264" : "H265");
    return 0;
}

void StreamRebroadcast::destroy_pipeline() {
    if (pipeline_) {
        gst_element_set_state(pipeline_, GST_STATE_NULL);
    }
    if (encoder_) { gst_object_unref(encoder_); encoder_ = NULL; }
    if (payloader_) { gst_object_unref(payloader_); payloader_ = NULL; }
    if (appsrc_) { gst_object_unref(appsrc_); appsrc_ = NULL; }
    if (pipeline_) { gst_object_unref(pipeline_); pipeline_ = NULL; }
}

std::string StreamRebroadcast::generate_sdp() {
    const char *codec_name = (out_codec_ == VideoCodec::H264) ? "H264" : "H265";
    char lip[INET_ADDRSTRLEN];
    struct in_addr a;
    a.s_addr = local_ip_;
    inet_ntop(AF_INET, &a, lip, sizeof(lip));

    uint32_t addr = ntohl(inet_addr(host_));
    bool is_multicast = ((addr & 0xF0000000) == 0xE0000000);

    const char *conn_fmt = is_multicast ? "c=IN IP4 %s/2\r\n" : "c=IN IP4 %s\r\n";

    char sdp[2048];
    int n = snprintf(sdp, sizeof(sdp),
        "v=0\r\n"
        "o=- %u 1 IN IP4 %s\r\n"
        "s=PixelPilot FPV Stream\r\n",
        session_id_, lip);
    if (n < 0 || (size_t)n >= sizeof(sdp)) return std::string();

    int r = snprintf(sdp + n, sizeof(sdp) - n, conn_fmt, host_);
    if (r < 0 || (size_t)r >= sizeof(sdp) - n) return std::string();
    n += r;

    r = snprintf(sdp + n, sizeof(sdp) - n,
        "t=0 0\r\n"
        "m=video %d RTP/AVP 96\r\n"
        "a=rtpmap:96 %s/90000\r\n",
        port_, codec_name);
    if (r < 0 || (size_t)r >= sizeof(sdp) - n) return std::string();
    n += r;

    // Add fmtp with sprop parameters from GStreamer payloader.
    // Always emit an a=fmtp line; VLC may refuse to connect without one.
    {
        std::lock_guard<std::mutex> lock(sprop_mtx_);
        if (out_codec_ == VideoCodec::H265) {
            std::string fmtp = "a=fmtp:96";
            bool has_params = false;
            if (!sprop_vps_.empty()) {
                fmtp += " sprop-vps=" + sprop_vps_;
                has_params = true;
            }
            if (!sprop_sps_.empty()) {
                fmtp += std::string(has_params ? "; " : " ");
                fmtp += "sprop-sps=" + sprop_sps_;
                has_params = true;
            }
            if (!sprop_pps_.empty()) {
                fmtp += std::string(has_params ? "; " : " ");
                fmtp += "sprop-pps=" + sprop_pps_;
                has_params = true;
            }
            fmtp += "\r\n";
            r = snprintf(sdp + n, sizeof(sdp) - n, "%s", fmtp.c_str());
            if (r >= 0 && (size_t)r < sizeof(sdp) - n) n += r;
        } else {
            std::string fmtp = "a=fmtp:96 packetization-mode=1";
            if (!sprop_sps_.empty() || !sprop_pps_.empty()) {
                fmtp += "; sprop-parameter-sets=";
                if (!sprop_sps_.empty()) fmtp += sprop_sps_;
                if (!sprop_pps_.empty()) fmtp += "," + sprop_pps_;
            }
            fmtp += "\r\n";
            r = snprintf(sdp + n, sizeof(sdp) - n, "%s", fmtp.c_str());
            if (r >= 0 && (size_t)r < sizeof(sdp) - n) n += r;
        }
    }

    return std::string(sdp, n);
}

void StreamRebroadcast::write_sdp_file() {
    const char *path = "/tmp/pixelpilot_rebroadcast.sdp";
    std::ofstream f(path);
    if (!f.is_open()) {
        spdlog::warn("Rebroadcast: could not write SDP file to {}", path);
        return;
    }
    f << sdp_;
    f.close();
    if (f.fail()) {
        spdlog::warn("Rebroadcast: failed to write SDP content to {}", path);
        return;
    }
    spdlog::info("Rebroadcast: SDP file written to {}", path);
    spdlog::info("Rebroadcast: you can also open the stream with: vlc {}", path);
}

int StreamRebroadcast::open_sap_socket() {
    sap_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sap_fd_ < 0) {
        spdlog::warn("Rebroadcast: failed to create SAP socket");
        return -1;
    }

    int ttl = 4;
    setsockopt(sap_fd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

    memset(&sap_addr_, 0, sizeof(sap_addr_));
    sap_addr_.sin_family = AF_INET;
    sap_addr_.sin_port = htons(9875);
    sap_addr_.sin_addr.s_addr = inet_addr("224.2.127.254");

    sap_msg_id_ = (uint16_t)(session_id_ & 0xFFFF);

    // Determine local IP by connecting a temporary UDP socket toward the destination
    int temp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (temp_fd >= 0) {
        struct sockaddr_in tmp;
        memset(&tmp, 0, sizeof(tmp));
        tmp.sin_family = AF_INET;
        tmp.sin_addr.s_addr = inet_addr(host_);
        tmp.sin_port = htons(port_);
        connect(temp_fd, (struct sockaddr *)&tmp, sizeof(tmp));
        struct sockaddr_in local;
        socklen_t local_len = sizeof(local);
        getsockname(temp_fd, (struct sockaddr *)&local, &local_len);
        local_ip_ = local.sin_addr.s_addr; // network byte order
        close(temp_fd);
    }

    spdlog::info("Rebroadcast: SAP announcements on 224.2.127.254:9875");
    return 0;
}

void StreamRebroadcast::close_sap_socket() {
    if (sap_fd_ >= 0) {
        close(sap_fd_);
        sap_fd_ = -1;
    }
}

void StreamRebroadcast::send_sap_announcement(bool deletion) {
    if (sap_fd_ < 0 || sdp_.empty()) return;

    // SAP header (RFC 2974): 8 bytes for IPv4
    uint8_t sap_header[8];
    sap_header[0] = 0x20 | (deletion ? 0x04 : 0x00); // V=1, T=deletion?
    sap_header[1] = 0;    // auth len = 0
    sap_header[2] = (sap_msg_id_ >> 8) & 0xFF;
    sap_header[3] = sap_msg_id_ & 0xFF;
    memcpy(&sap_header[4], &local_ip_, 4); // originating source

    // Payload: "application/sdp\0" + SDP content
    const char *mime = "application/sdp";
    size_t mime_len = strlen(mime) + 1; // include null terminator
    size_t total = 8 + mime_len + sdp_.size();
    std::vector<uint8_t> packet(total);
    memcpy(packet.data(), sap_header, 8);
    memcpy(packet.data() + 8, mime, mime_len);
    memcpy(packet.data() + 8 + mime_len, sdp_.data(), sdp_.size());

    sendto(sap_fd_, packet.data(), total, 0,
           (struct sockaddr *)&sap_addr_, sizeof(sap_addr_));
}

void StreamRebroadcast::loop() {
    while (true) {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this] { return !this->queue_.empty(); });
        if (queue_.empty()) {
            break;
        }
        rebroadcast_rpc rpc = queue_.front();
        queue_.pop();
        lock.unlock();

        switch (rpc.command) {
        case rebroadcast_rpc::RPC_START:
            {
                SPDLOG_DEBUG("Rebroadcast: got RPC START");
                if (running_) break;
                if (build_pipeline() == 0) {
                    running_ = true;
                    rebroadcast_enabled.store(1);
                    open_sap_socket();
                    sdp_ = generate_sdp();
                    write_sdp_file();
                    send_sap_announcement(false);
                    last_sap_time_ = std::chrono::steady_clock::now();
                    osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, true);
                    if (transcode_) {
                        osd_publish_uint_fact("rebroadcast.bitrate", NULL, 0, bitrate_);
                    }
                    spdlog::info("Rebroadcast: started");
                }
                break;
            }
        case rebroadcast_rpc::RPC_STOP:
            {
                SPDLOG_DEBUG("Rebroadcast: got RPC STOP");
                if (!running_) break;
                send_sap_announcement(true);
                close_sap_socket();
                destroy_pipeline();
                running_ = false;
                rebroadcast_enabled.store(0);
                osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, false);
                spdlog::info("Rebroadcast: stopped");
                break;
            }
        case rebroadcast_rpc::RPC_TOGGLE:
            {
                SPDLOG_DEBUG("Rebroadcast: got RPC TOGGLE");
                if (!running_) {
                    if (build_pipeline() == 0) {
                        running_ = true;
                        rebroadcast_enabled.store(1);
                        open_sap_socket();
                        sdp_ = generate_sdp();
                        write_sdp_file();
                        send_sap_announcement(false);
                        last_sap_time_ = std::chrono::steady_clock::now();
                        osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, true);
                        if (transcode_) {
                            osd_publish_uint_fact("rebroadcast.bitrate", NULL, 0, bitrate_);
                        }
                        spdlog::info("Rebroadcast: started");
                    }
                } else {
                    send_sap_announcement(true);
                    close_sap_socket();
                    destroy_pipeline();
                    running_ = false;
                    rebroadcast_enabled.store(0);
                    osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, false);
                    spdlog::info("Rebroadcast: stopped");
                }
                break;
            }
        case rebroadcast_rpc::RPC_SET_BITRATE:
            {
                SPDLOG_DEBUG("Rebroadcast: got RPC SET_BITRATE");
                if (encoder_ && bitrate_ > 0) {
                    g_object_set(encoder_, "bps", bitrate_, NULL);
                    spdlog::info("Rebroadcast: bitrate set to {} bps", bitrate_);
                    osd_publish_uint_fact("rebroadcast.bitrate", NULL, 0, bitrate_);
                }
                break;
            }
        case rebroadcast_rpc::RPC_FRAME:
            {
                if (!running_ || !appsrc_) break;

                auto frame_data = rpc.frame;

                // Wait for a keyframe before starting to push data.
                // Without VPS/SPS/PPS (which accompany keyframes), the
                // receiving decoder cannot initialize and will produce
                // "invalid NALU" / "Could not find ref" errors.
                if (!idr_seen_) {
                    if (frame_is_keyframe(frame_data->data(), frame_data->size(), codec_)) {
                        idr_seen_ = true;
                        spdlog::info("Rebroadcast: keyframe detected, starting stream");
                    } else {
                        break;
                    }
                }

                GstBuffer *buf = gst_buffer_new_allocate(NULL, frame_data->size(), NULL);
                gst_buffer_fill(buf, 0, frame_data->data(), frame_data->size());

                // Mark the first buffer as a discontinuity so downstream
                // elements (parser, payloader) know to resynchronize.
                if (frame_count_ == 0) {
                    GST_BUFFER_FLAG_SET(buf, GST_BUFFER_FLAG_DISCONT);
                }

                GstFlowReturn ret = gst_app_src_push_buffer(GST_APP_SRC(appsrc_), buf);

                frame_count_++;
                if (frame_count_ <= 5 || (frame_count_ % 300 == 0)) {
                    spdlog::info("Rebroadcast: frame #{} size={} push={}",
                                 frame_count_, frame_data->size(), (int)ret);
                }
                if (ret != GST_FLOW_OK) {
                    if (ret == GST_FLOW_EOS) {
                        spdlog::warn("Rebroadcast: pipeline reached EOS on frame #{}", frame_count_);
                    } else {
                        SPDLOG_DEBUG("Rebroadcast: push_buffer returned {}", (int)ret);
                    }
                }

                // Check for sprop update immediately (don't wait for SAP
                // interval) so VLC can init from the SDP as soon as
                // the payloader provides codec parameters.
                {
                    bool need_sdp_update = false;
                    {
                        std::lock_guard<std::mutex> slock(sprop_mtx_);
                        need_sdp_update = sprop_updated_;
                        sprop_updated_ = false;
                    }
                    if (need_sdp_update) {
                        sdp_ = generate_sdp();
                        write_sdp_file();
                        send_sap_announcement(false);
                        last_sap_time_ = std::chrono::steady_clock::now();
                        spdlog::info("Rebroadcast: SDP updated with codec parameters");
                    }
                }

                // Periodically send SAP announcements
                auto now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::seconds>(now - last_sap_time_).count() >= SAP_INTERVAL_SEC) {
                    send_sap_announcement(false);
                    last_sap_time_ = now;
                }
                break;
            }
        case rebroadcast_rpc::RPC_SHUTDOWN:
            goto end;
        }
    }
end:
    if (running_) {
        send_sap_announcement(true);
        close_sap_socket();
        destroy_pipeline();
        running_ = false;
        rebroadcast_enabled.store(0);
    }
    spdlog::info("Rebroadcast thread done.");
}

// C-compatible interface
extern "C" {
    int rebroadcast_is_enabled(void) {
        return rebroadcast_enabled.load();
    }

    void rebroadcast_start(StreamRebroadcast* rb) {
        if (rb) rb->start();
    }

    void rebroadcast_stop(StreamRebroadcast* rb) {
        if (rb) rb->stop();
    }

    void rebroadcast_toggle(StreamRebroadcast* rb) {
        if (rb) rb->toggle();
    }

    void rebroadcast_set_bitrate(StreamRebroadcast* rb, int bps) {
        if (rb) rb->set_bitrate(bps);
    }
}
