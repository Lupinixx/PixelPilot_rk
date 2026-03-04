#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <chrono>
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

StreamRebroadcast::StreamRebroadcast(rebroadcast_params params) {
    host_ = params.host;
    port_ = params.port;
    codec_ = params.codec;
    sock_fd_ = -1;
    rtp_seq_ = 0;
    rtp_timestamp_ = 0;
    // Generate a random SSRC to avoid collisions with other streams
    srand(time(NULL) ^ getpid());
    rtp_ssrc_ = (uint32_t)rand();
    running_ = false;
}

StreamRebroadcast::~StreamRebroadcast() {
    if (sock_fd_ >= 0) {
        close_socket();
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

// Build and send an RTP packet with H264/H265 NAL unit payload.
// For simplicity, we send the raw bytestream NALs directly as RTP payload.
// This uses a simple single-NAL unit packetization approach with fragmentation
// for NALs larger than MAX_RTP_PAYLOAD.
void StreamRebroadcast::send_rtp_packet(const uint8_t *data, size_t size) {
    if (sock_fd_ < 0 || size == 0) return;

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

    for (auto& nal : nals) {
        const uint8_t *nal_data = data + nal.first;
        size_t nal_size = nal.second;
        if (nal_size == 0) continue;

        if (nal_size <= MAX_RTP_PAYLOAD) {
            // Single NAL unit packet
            uint8_t packet[RTP_HEADER_SIZE + MAX_RTP_PAYLOAD];
            // RTP Header
            packet[0] = 0x80; // V=2, P=0, X=0, CC=0
            packet[1] = 96 | 0x80; // PT=96 (dynamic), M=1 (marker)
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

                    uint8_t packet[RTP_HEADER_SIZE + MAX_RTP_PAYLOAD];
                    packet[0] = 0x80;
                    packet[1] = 96 | (last ? 0x80 : 0x00); // marker on last
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

                    uint8_t packet[RTP_HEADER_SIZE + MAX_RTP_PAYLOAD];
                    packet[0] = 0x80;
                    packet[1] = 96 | (last ? 0x80 : 0x00);
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

    // Use wall-clock timing for RTP timestamps (90kHz clock)
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
    rtp_timestamp_ = (uint32_t)((elapsed * 90) / 1000); // 90kHz clock
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
                    osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, true);
                    spdlog::info("Rebroadcast: started");
                }
                break;
            }
        case rebroadcast_rpc::RPC_STOP:
            {
                SPDLOG_DEBUG("Rebroadcast: got RPC STOP");
                if (!running_) break;
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
                        osd_publish_bool_fact("rebroadcast.enabled", NULL, 0, true);
                        spdlog::info("Rebroadcast: started");
                    }
                } else {
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
                break;
            }
        case rebroadcast_rpc::RPC_SHUTDOWN:
            goto end;
        }
    }
end:
    if (running_) {
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
