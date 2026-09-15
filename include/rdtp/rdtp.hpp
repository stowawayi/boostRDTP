// RDTP - Reliable Data Transfer Protocol
// SPDX-License-Identifier: MIT
//
// This library is inspired by concepts from US Patent US6831912B1,
// originally invented by Jon H. Sherman and assigned to Raytheon Company.
// The patent has expired and the implementation provided here is
// open source under the MIT License.

#ifndef __RDTP_HPP__
#define __RDTP_HPP__

#include <boost/asio.hpp>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <vector>

namespace rdtp {

constexpr uint32_t RDTP_SYNC = 0xC01DBEEFu;

// Data-channel packet header (UDP), 36 bytes on the wire, big-endian except
// data_type, which is passed through unswapped.
struct PacketHeader {
    uint32_t sync = RDTP_SYNC;
    uint32_t header_size = 36;               // byte count, not a word count
    uint32_t datagram_sequence = 0;
    uint32_t data_type = 0;                  // user-defined tag; not byte-swapped on the wire
    uint32_t oldest_datagram_available = 0;  // sender's retransmission low-water mark
    uint32_t datagram_byte_size = 0;
    uint32_t file_id = 0;
    uint32_t file_segment_no = 0;            // 1-based: first segment of a file is 1, not 0
    uint32_t segments_in_file = 0;

    static constexpr size_t wire_size = 36;

    void to_wire(uint8_t* buf) const;
    static PacketHeader from_wire(const uint8_t* buf);
};

// Represents a complete datagram
struct Datagram {
    PacketHeader header;
    std::vector<uint8_t> data;
};

// Control channel (TCP). Every control message, both directions, shares
// this 16-byte header.
enum class ControlMsgType : uint32_t {
    InitRequest = 0,  // receiver -> sender, first message on the control connection
    InitReply = 1,    // sender -> receiver, reply to InitRequest
    Ack = 2,          // receiver -> sender, periodic
};

struct ControlMsgHeader {
    uint32_t sync = RDTP_SYNC;
    uint32_t size = 0;                        // payload byte count following this header
    ControlMsgType msg_type = ControlMsgType::InitRequest;
    uint32_t cntr = 0;                        // per-channel monotonically increasing message counter

    static constexpr size_t wire_size = 16;

    void to_wire(uint8_t* buf) const;
    static ControlMsgHeader from_wire(const uint8_t* buf);
};

// Receiver -> sender: ack-window parameters plus the UDP port the receiver
// bound for the data channel, so the sender knows where to send datagrams.
struct InitRequestPayload {
    uint32_t ack_window_size = 0;
    uint32_t ack_window_timeout = 0;
    uint32_t receiver_data_port = 0;

    static constexpr size_t wire_size = 12;

    void to_wire(uint8_t* buf) const;
    static InitRequestPayload from_wire(const uint8_t* buf);
};

// Sender -> receiver: reply to InitRequest.
struct InitReplyPayload {
    uint32_t max_packet_size = 0;
    uint32_t init_sequence_no = 0;

    static constexpr size_t wire_size = 8;

    void to_wire(uint8_t* buf) const;
    static InitReplyPayload from_wire(const uint8_t* buf);
};

// Receiver -> sender, periodic.
struct AckPayload {
    uint32_t recv_window = 0;
    uint32_t last_dg_acked = 0;              // highest contiguously-received sequence number
    std::vector<uint32_t> missing_seq_nums;  // NACK list; wire-prefixed with its own count

    std::vector<uint8_t> to_wire() const;
    static AckPayload from_wire(const uint8_t* buf, size_t len);
};

// Handler signatures
using AckHandler = std::function<void(const std::vector<uint32_t>& missing_seq_nums)>;
using DataHandler = std::function<void(const Datagram& datagram)>;

// Receiver role: connects out to the sender, drives the init handshake, and
// sends acks/nacks back over the TCP control channel.
class RDTPClient {
public:
    RDTPClient(boost::asio::io_context& io_context, const std::string& server_ip,
               uint16_t server_data_port, uint16_t server_control_port = 0);

    // Opens the TCP control connection. Must be called before send_init().
    // If server_control_port was 0 at construction, defaults to server_data_port + 1.
    void connect();

    // Sends an InitRequest and blocks for the InitReply, capturing
    // init_sequence_no as the starting sequence number.
    void send_init(uint32_t ack_window_size, uint32_t ack_window_timeout);

    // Blocking UDP receive loop for data-channel datagrams. Drops any
    // datagram that fails sync/header_size validation.
    void receive_loop(DataHandler on_data);

    // Sends a structured Ack over the TCP control channel. last_dg_acked is
    // computed automatically from contiguous receipt tracking;
    // missing_seq_nums is caller-supplied.
    void send_ack(const std::vector<uint32_t>& missing_seq_nums);

    // Closes both sockets so a receive_loop() running on another thread
    // returns instead of blocking forever.
    void stop();

    uint32_t corrupted_packet_count() const { return corrupted_packet_count_; }

private:
    boost::asio::ip::udp::socket data_socket_;
    boost::asio::ip::udp::endpoint server_data_endpoint_;
    boost::asio::ip::tcp::socket control_socket_;
    boost::asio::ip::tcp::endpoint server_control_endpoint_;

    uint32_t init_sequence_no_ = 0;
    uint32_t next_expected_seq_ = 0;   // lowest sequence number not yet fully received
    uint32_t control_msg_cntr_ = 0;
    uint32_t ack_window_size_ = 0;
    uint32_t corrupted_packet_count_ = 0;

    std::map<uint32_t, Datagram> received_packets_;
};

// Sender role: listens for the receiver's TCP control connection, replies to
// the init handshake, sends file data over UDP, and retransmits on NACK.
class RDTPServer {
public:
    RDTPServer(boost::asio::io_context& io_context, uint16_t data_port, uint16_t control_port = 0);

    // Blocks until a receiver's TCP control connection arrives, then
    // performs the init handshake. If control_port was 0 at construction,
    // defaults to data_port + 1.
    void accept();

    // Blocking loop reading Ack messages off the control channel. Retransmits
    // any NACKed sequence numbers still tracked, invokes on_ack, and advances
    // the retransmission low-water mark. Intended to run on its own thread
    // (see examples/server.cpp), since send_datagram() is meant to be called
    // concurrently from the main thread.
    void run_ack_loop(AckHandler on_ack);

    // Sends one file segment over UDP. Tracks it for possible retransmission
    // and patches oldest_datagram_available before sending.
    void send_datagram(Datagram datagram);

    // Closes the acceptor and both sockets so a blocking accept() or
    // run_ack_loop() running on another thread returns instead of blocking
    // forever.
    void stop();

    uint32_t max_packet_size() const { return max_packet_size_; }

private:
    void retransmit(uint32_t seq_no);  // caller must hold tracked_mutex_

    boost::asio::ip::udp::socket data_socket_;
    boost::asio::ip::udp::endpoint client_data_endpoint_;
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ip::tcp::socket control_socket_;

    uint32_t sequence_counter_ = 0;
    uint32_t control_msg_cntr_ = 0;
    uint32_t max_packet_size_ = 4096;

    std::mutex tracked_mutex_;
    std::map<uint32_t, Datagram> tracked_unacked_;  // sent-but-not-yet-acked, keyed by datagram_sequence
};

} // namespace rdtp

#endif // __RDTP_HPP__
