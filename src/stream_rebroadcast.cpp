#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <chrono>
#include <fstream>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "spdlog/spdlog.h"

#include "stream_rebroadcast.h"

extern "C" {
#include "osd.h"
}

std::atomic<int> rebroadcast_enabled{0};

// RTP header size
static const int RTP_HEADER_SIZE = 12;
// Max UDP payload to avoid fragmentation
static const int MAX_RTP_PAYLOAD = 1400;
// SAP announcement interval in seconds
static const int SAP_INTERVAL_SEC = 5;

StreamRebroadcast::StreamRebroadcast(rebroadcast_params params) {
    host_ = params.host;
    port_ = params.port;
    codec_ = params.codec;
    sock_fd_ = -1;
    sap_fd_ = -1;
    local_ip_ = 0;
    sap_msg_id_ = 0;
    // Generate random initial values per RFC 3550
    srand(time(NULL) ^ getpid());
    rtp_seq_ = (uint16_t)(rand() & 0xFFFF);
    rtp_timestamp_ = 0;
    rtp_ssrc_ = (uint32_t)rand();
    running_ = false;
}

StreamRebroadcast::~StreamRebroadcast() {
    if (sock_fd_ >= 0) {
        close_socket();
    }
    if (sap_fd_ >= 0) {
        close_sap_socket();
    }
}

void StreamRebroadcast::frame(std::shared_ptr<std::vector<uint8_t>> frame) {
    rebroadcast_rpc rpc = {
        .command = rebroadcast_rpc::RPC_FRAME,
        .frame = frame
    };
    enqueue(rpc);
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

int StreamRebroadcast::open_socket() {
    sock_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd_ < 0) {
        spdlog::error("Rebroadcast: failed to create UDP socket");
        return -1;
    }

    // Allow broadcast
    int broadcast_enable = 1;
    setsockopt(sock_fd_, SOL_SOCKET, SO_BROADCAST, &broadcast_enable, sizeof(broadcast_enable));

    // If multicast address, set TTL and interface
    uint32_t addr = ntohl(inet_addr(host_));
    if ((addr & 0xF0000000) == 0xE0000000) {
        int ttl = 2;
        setsockopt(sock_fd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
    }

    memset(&dest_addr_, 0, sizeof(dest_addr_));
    dest_addr_.sin_family = AF_INET;
    dest_addr_.sin_port = htons(port_);
    dest_addr_.sin_addr.s_addr = inet_addr(host_);

    spdlog::info("Rebroadcast: streaming to {}:{}", host_, port_);
    return 0;
}

void StreamRebroadcast::close_socket() {
    if (sock_fd_ >= 0) {
        close(sock_fd_);
        sock_fd_ = -1;
    }
}

std::string StreamRebroadcast::generate_sdp() {
    const char *codec_name = (codec_ == VideoCodec::H264) ? "H264" : "H265";
    char lip[INET_ADDRSTRLEN];
    struct in_addr a;
    a.s_addr = local_ip_;
    inet_ntop(AF_INET, &a, lip, sizeof(lip));

    // Detect if destination is multicast
    uint32_t addr = ntohl(inet_addr(host_));
    bool is_multicast = ((addr & 0xF0000000) == 0xE0000000);

    const char *conn_fmt = is_multicast ? "c=IN IP4 %s/2\r\n" : "c=IN IP4 %s\r\n";

    char sdp[1024];
    int n = snprintf(sdp, sizeof(sdp),
        "v=0\r\n"
        "o=- %u 1 IN IP4 %s\r\n"
        "s=PixelPilot FPV Stream\r\n",
        rtp_ssrc_, lip);
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

    if (codec_ == VideoCodec::H264) {
        r = snprintf(sdp + n, sizeof(sdp) - n,
            "a=fmtp:96 packetization-mode=1\r\n");
        if (r < 0 || (size_t)r >= sizeof(sdp) - n) return std::string();
        n += r;
    }

    return std::string(sdp);
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

    sap_msg_id_ = (uint16_t)(rtp_ssrc_ & 0xFFFF);

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

// Send a single (small) NAL unit as one RTP packet.
void StreamRebroadcast::send_single_nal_rtp(const uint8_t *nal_data,
                                            size_t nal_size, bool marker) {
    if (sock_fd_ < 0 || nal_size == 0 || nal_size > MAX_RTP_PAYLOAD) return;

    uint8_t packet[RTP_HEADER_SIZE + MAX_RTP_PAYLOAD];
    packet[0] = 0x80;
    packet[1] = 96 | (marker ? 0x80 : 0x00);
    packet[2] = (rtp_seq_ >> 8) & 0xFF;
    packet[3] = rtp_seq_ & 0xFF;
    uint32_t ts = rtp_timestamp_;
    packet[4] = (ts >> 24) & 0xFF;
    packet[5] = (ts >> 16) & 0xFF;
    packet[6] = (ts >> 8) & 0xFF;
    packet[7] = ts & 0xFF;
    packet[8] = (rtp_ssrc_ >> 24) & 0xFF;
    packet[9] = (rtp_ssrc_ >> 16) & 0xFF;
    packet[10] = (rtp_ssrc_ >> 8) & 0xFF;
    packet[11] = rtp_ssrc_ & 0xFF;

    memcpy(packet + RTP_HEADER_SIZE, nal_data, nal_size);
    rtp_seq_++;

    sendto(sock_fd_, packet, RTP_HEADER_SIZE + nal_size, 0,
           (struct sockaddr *)&dest_addr_, sizeof(dest_addr_));
}

// Build and send an RTP packet with H264/H265 NAL unit payload.
// For simplicity, we send the raw bytestream NALs directly as RTP payload.
// This uses a simple single-NAL unit packetization approach with fragmentation
// for NALs larger than MAX_RTP_PAYLOAD.
void StreamRebroadcast::send_rtp_packet(const uint8_t *data, size_t size) {
    if (sock_fd_ < 0 || size == 0) return;

    // Compute timestamp at the start so every NAL in this frame shares
    // the same value and the very first frame never has timestamp 0.
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
    rtp_timestamp_ = (uint32_t)((elapsed * 90) / 1000); // 90kHz clock

    // Find NAL units in the bytestream (separated by 00 00 00 01 or 00 00 01)
    size_t pos = 0;
    std::vector<std::pair<size_t, size_t>> nals; // offset, length

    while (pos < size) {
        // Find start code
        size_t start = pos;
        bool found = false;
        while (pos + 3 <= size) {
            if (pos + 4 <= size && data[pos] == 0 && data[pos+1] == 0 && data[pos+2] == 0 && data[pos+3] == 1) {
                if (start != pos && !nals.empty()) {
                    nals.back().second = pos - nals.back().first;
                }
                pos += 4;
                nals.push_back({pos, 0});
                found = true;
                break;
            } else if (data[pos] == 0 && data[pos+1] == 0 && data[pos+2] == 1) {
                if (start != pos && !nals.empty()) {
                    nals.back().second = pos - nals.back().first;
                }
                pos += 3;
                nals.push_back({pos, 0});
                found = true;
                break;
            }
            pos++;
        }
        if (!found) {
            if (!nals.empty()) {
                nals.back().second = size - nals.back().first;
            }
            break;
        }
    }

    // If no NAL units found, send as single packet
    if (nals.empty()) {
        nals.push_back({0, size});
    }

    // Detect and cache VPS/SPS/PPS parameter sets so we can prepend them
    // to non-keyframe access units.  This lets VLC (and other players)
    // initialise the decoder even when they join mid-stream.
    bool has_param_sets = false;
    for (size_t i = 0; i < nals.size(); i++) {
        const uint8_t *nd = data + nals[i].first;
        size_t ns = nals[i].second;
        if (ns < 2) continue;

        if (codec_ == VideoCodec::H265) {
            uint8_t nt = (nd[0] >> 1) & 0x3F;
            if (nt == 32) { cached_vps_.assign(nd, nd + ns); has_param_sets = true; }
            else if (nt == 33) { cached_sps_.assign(nd, nd + ns); has_param_sets = true; }
            else if (nt == 34) { cached_pps_.assign(nd, nd + ns); has_param_sets = true; }
        } else {
            uint8_t nt = nd[0] & 0x1F;
            if (nt == 7) { cached_sps_.assign(nd, nd + ns); has_param_sets = true; }
            else if (nt == 8) { cached_pps_.assign(nd, nd + ns); has_param_sets = true; }
        }
    }

    // If this access unit lacks parameter sets, prepend the cached ones so
    // that a player joining at this point can still decode.
    if (!has_param_sets) {
        if (codec_ == VideoCodec::H265 && !cached_vps_.empty())
            send_single_nal_rtp(cached_vps_.data(), cached_vps_.size(), false);
        if (!cached_sps_.empty())
            send_single_nal_rtp(cached_sps_.data(), cached_sps_.size(), false);
        if (!cached_pps_.empty())
            send_single_nal_rtp(cached_pps_.data(), cached_pps_.size(), false);
    }

    for (size_t nal_idx = 0; nal_idx < nals.size(); nal_idx++) {
        const uint8_t *nal_data = data + nals[nal_idx].first;
        size_t nal_size = nals[nal_idx].second;
        bool is_last_nal = (nal_idx == nals.size() - 1);
        if (nal_size == 0) continue;

        if (nal_size <= MAX_RTP_PAYLOAD) {
            // Single NAL unit packet
            uint8_t packet[RTP_HEADER_SIZE + MAX_RTP_PAYLOAD];
            // RTP Header
            packet[0] = 0x80; // V=2, P=0, X=0, CC=0
            packet[1] = 96 | (is_last_nal ? 0x80 : 0x00); // PT=96 (dynamic), M=1 only on last NAL of frame
            packet[2] = (rtp_seq_ >> 8) & 0xFF;
            packet[3] = rtp_seq_ & 0xFF;
            uint32_t ts = rtp_timestamp_;
            packet[4] = (ts >> 24) & 0xFF;
            packet[5] = (ts >> 16) & 0xFF;
            packet[6] = (ts >> 8) & 0xFF;
            packet[7] = ts & 0xFF;
            packet[8] = (rtp_ssrc_ >> 24) & 0xFF;
            packet[9] = (rtp_ssrc_ >> 16) & 0xFF;
            packet[10] = (rtp_ssrc_ >> 8) & 0xFF;
            packet[11] = rtp_ssrc_ & 0xFF;

            memcpy(packet + RTP_HEADER_SIZE, nal_data, nal_size);
            rtp_seq_++;

            sendto(sock_fd_, packet, RTP_HEADER_SIZE + nal_size, 0,
                   (struct sockaddr *)&dest_addr_, sizeof(dest_addr_));
        } else {
            // FU-A / FU fragmentation for large NAL units
            if (codec_ == VideoCodec::H264) {
                uint8_t nal_header = nal_data[0];
                uint8_t nal_type = nal_header & 0x1F;
                uint8_t nri = nal_header & 0x60;
                size_t offset = 1; // skip NAL header byte

                bool first = true;
                while (offset < nal_size) {
                    size_t chunk = std::min((size_t)MAX_RTP_PAYLOAD - 2, nal_size - offset);
                    bool last = (offset + chunk >= nal_size);
                    bool marker = last && is_last_nal;

                    uint8_t packet[RTP_HEADER_SIZE + MAX_RTP_PAYLOAD];
                    packet[0] = 0x80;
                    packet[1] = 96 | (marker ? 0x80 : 0x00); // marker only on last fragment of last NAL
                    packet[2] = (rtp_seq_ >> 8) & 0xFF;
                    packet[3] = rtp_seq_ & 0xFF;
                    uint32_t ts = rtp_timestamp_;
                    packet[4] = (ts >> 24) & 0xFF;
                    packet[5] = (ts >> 16) & 0xFF;
                    packet[6] = (ts >> 8) & 0xFF;
                    packet[7] = ts & 0xFF;
                    packet[8] = (rtp_ssrc_ >> 24) & 0xFF;
                    packet[9] = (rtp_ssrc_ >> 16) & 0xFF;
                    packet[10] = (rtp_ssrc_ >> 8) & 0xFF;
                    packet[11] = rtp_ssrc_ & 0xFF;

                    // FU indicator: type=28 (FU-A), NRI from original
                    packet[RTP_HEADER_SIZE] = nri | 28;
                    // FU header
                    uint8_t fu_header = nal_type;
                    if (first) fu_header |= 0x80; // S bit
                    if (last) fu_header |= 0x40;  // E bit
                    packet[RTP_HEADER_SIZE + 1] = fu_header;

                    memcpy(packet + RTP_HEADER_SIZE + 2, nal_data + offset, chunk);
                    rtp_seq_++;

                    sendto(sock_fd_, packet, RTP_HEADER_SIZE + 2 + chunk, 0,
                           (struct sockaddr *)&dest_addr_, sizeof(dest_addr_));

                    offset += chunk;
                    first = false;
                }
            } else {
                // H265 FU fragmentation
                uint8_t nal_header0 = nal_data[0];
                uint8_t nal_header1 = nal_data[1];
                uint8_t nal_type = (nal_header0 >> 1) & 0x3F;
                size_t offset = 2; // skip 2-byte NAL header

                bool first = true;
                while (offset < nal_size) {
                    size_t chunk = std::min((size_t)MAX_RTP_PAYLOAD - 3, nal_size - offset);
                    bool last = (offset + chunk >= nal_size);
                    bool marker = last && is_last_nal;

                    uint8_t packet[RTP_HEADER_SIZE + MAX_RTP_PAYLOAD];
                    packet[0] = 0x80;
                    packet[1] = 96 | (marker ? 0x80 : 0x00);
                    packet[2] = (rtp_seq_ >> 8) & 0xFF;
                    packet[3] = rtp_seq_ & 0xFF;
                    uint32_t ts = rtp_timestamp_;
                    packet[4] = (ts >> 24) & 0xFF;
                    packet[5] = (ts >> 16) & 0xFF;
                    packet[6] = (ts >> 8) & 0xFF;
                    packet[7] = ts & 0xFF;
                    packet[8] = (rtp_ssrc_ >> 24) & 0xFF;
                    packet[9] = (rtp_ssrc_ >> 16) & 0xFF;
                    packet[10] = (rtp_ssrc_ >> 8) & 0xFF;
                    packet[11] = rtp_ssrc_ & 0xFF;

                    // FU indicator (PayloadHdr): type=49 (FU), keep layer/TID
                    packet[RTP_HEADER_SIZE] = (nal_header0 & 0x81) | (49 << 1);
                    packet[RTP_HEADER_SIZE + 1] = nal_header1;
                    // FU header
                    uint8_t fu_header = nal_type;
                    if (first) fu_header |= 0x80;
                    if (last) fu_header |= 0x40;
                    packet[RTP_HEADER_SIZE + 2] = fu_header;

                    memcpy(packet + RTP_HEADER_SIZE + 3, nal_data + offset, chunk);
                    rtp_seq_++;

                    sendto(sock_fd_, packet, RTP_HEADER_SIZE + 3 + chunk, 0,
                           (struct sockaddr *)&dest_addr_, sizeof(dest_addr_));

                    offset += chunk;
                    first = false;
                }
            }
        }
    }
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
                if (open_socket() == 0) {
                    running_ = true;
                    rebroadcast_enabled.store(1);
                    open_sap_socket();
                    sdp_ = generate_sdp();
                    write_sdp_file();
                    send_sap_announcement(false);
                    last_sap_time_ = std::chrono::steady_clock::now();
                    osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, true);
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
                close_socket();
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
                    if (open_socket() == 0) {
                        running_ = true;
                        rebroadcast_enabled.store(1);
                        open_sap_socket();
                        sdp_ = generate_sdp();
                        write_sdp_file();
                        send_sap_announcement(false);
                        last_sap_time_ = std::chrono::steady_clock::now();
                        osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, true);
                        spdlog::info("Rebroadcast: started");
                    }
                } else {
                    send_sap_announcement(true);
                    close_sap_socket();
                    close_socket();
                    running_ = false;
                    rebroadcast_enabled.store(0);
                    osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, false);
                    spdlog::info("Rebroadcast: stopped");
                }
                break;
            }
        case rebroadcast_rpc::RPC_FRAME:
            {
                if (!running_) break;
                std::shared_ptr<std::vector<uint8_t>> frame = rpc.frame;
                send_rtp_packet(frame->data(), frame->size());
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
        close_socket();
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
}
