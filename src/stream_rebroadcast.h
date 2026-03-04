#ifndef STREAM_REBROADCAST_H
#define STREAM_REBROADCAST_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include <netinet/in.h>

#include "gstrtpreceiver.h"

struct rebroadcast_rpc {
    enum {
        RPC_FRAME,
        RPC_START,
        RPC_STOP,
        RPC_TOGGLE,
        RPC_SHUTDOWN
    } command;
    std::shared_ptr<std::vector<uint8_t>> frame;
};

struct rebroadcast_params {
    const char *host = "224.0.0.1";
    int port = 5700;
    VideoCodec codec = VideoCodec::H265;
};

extern std::atomic<int> rebroadcast_enabled;

class StreamRebroadcast {
public:
    explicit StreamRebroadcast(rebroadcast_params params);
    virtual ~StreamRebroadcast();

    void frame(std::shared_ptr<std::vector<uint8_t>> frame);
    void start();
    void stop();
    void toggle();
    void shutdown();

    static void *__THREAD__(void *context);
private:
    void enqueue(rebroadcast_rpc rpc);
    void loop();
    int open_socket();
    void close_socket();
    void send_rtp_packet(const uint8_t *data, size_t size);
    void write_sdp_file();

    std::queue<rebroadcast_rpc> queue_;
    std::mutex mtx_;
    std::condition_variable cv_;

    const char *host_;
    int port_;
    VideoCodec codec_;
    int sock_fd_;
    struct sockaddr_in dest_addr_;
    uint16_t rtp_seq_;
    uint32_t rtp_timestamp_;
    uint32_t rtp_ssrc_;
    bool running_;
    std::string sdp_path_;
};
#else
typedef struct StreamRebroadcast StreamRebroadcast;
#endif

#ifdef __cplusplus
extern "C" {
#endif

int rebroadcast_is_enabled(void);
void rebroadcast_start(StreamRebroadcast* rb);
void rebroadcast_stop(StreamRebroadcast* rb);
void rebroadcast_toggle(StreamRebroadcast* rb);

#ifdef __cplusplus
}
#endif

#endif
