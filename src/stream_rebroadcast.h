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
#include <chrono>
#include <netinet/in.h>

#include <gst/gst.h>
#include <gst/app/gstappsrc.h>

#include "gstrtpreceiver.h"

struct rebroadcast_rpc {
    enum {
        RPC_FRAME,
        RPC_START,
        RPC_STOP,
        RPC_TOGGLE,
        RPC_SET_BITRATE,
        RPC_SHUTDOWN
    } command;
    std::shared_ptr<std::vector<uint8_t>> frame;
};

struct rebroadcast_params {
    const char *host = "224.0.0.1";
    int port = 5700;
    VideoCodec codec = VideoCodec::H265;
    int bitrate = 4000000;  // 4 Mbps default for transcode mode
    bool transcode = false;
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
    void set_bitrate(int bps);
    void shutdown();

    static void *__THREAD__(void *context);
private:
    void enqueue(rebroadcast_rpc rpc);
    void loop();

    // GStreamer pipeline management
    int build_pipeline();
    void destroy_pipeline();
    static GstBusSyncReply bus_sync_handler(GstBus *bus, GstMessage *msg, gpointer data);
    static void on_caps_change(GstPad *pad, GParamSpec *pspec, gpointer data);

    // SDP/SAP
    std::string generate_sdp();
    void write_sdp_file();
    int open_sap_socket();
    void close_sap_socket();
    void send_sap_announcement(bool deletion);

    std::queue<rebroadcast_rpc> queue_;
    std::mutex mtx_;
    std::condition_variable cv_;

    const char *host_;
    int port_;
    VideoCodec codec_;       // Input codec
    VideoCodec out_codec_;   // Output codec (H265 for transcode, same as input for passthrough)
    int bitrate_;
    bool transcode_;
    bool running_;
    uint32_t session_id_;

    // GStreamer elements
    GstElement *pipeline_;
    GstElement *appsrc_;
    GstElement *payloader_;
    GstElement *encoder_;

    // SAP announcement state
    int sap_fd_;
    struct sockaddr_in sap_addr_;
    uint16_t sap_msg_id_;
    uint32_t local_ip_;
    std::string sdp_;
    std::chrono::steady_clock::time_point last_sap_time_;

    // Codec parameters extracted from GStreamer payloader (for SDP)
    std::mutex sprop_mtx_;
    std::string sprop_vps_;
    std::string sprop_sps_;
    std::string sprop_pps_;
    bool sprop_updated_;
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
void rebroadcast_set_bitrate(StreamRebroadcast* rb, int bps);

#ifdef __cplusplus
}
#endif

#endif
