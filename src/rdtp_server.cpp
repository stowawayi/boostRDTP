#include "rdtp/rdtp.hpp"
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace {

boost::asio::ip::udp::endpoint udp_bind_endpoint(const std::string& bind_address, uint16_t port) {
    return boost::asio::ip::udp::endpoint(boost::asio::ip::make_address(bind_address), port);
}

boost::asio::ip::tcp::endpoint tcp_bind_endpoint(const std::string& bind_address, uint16_t port) {
    return boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(bind_address), port);
}

} // namespace

namespace rdtp {

RDTPServer::RDTPServer(boost::asio::io_context& io_context, uint16_t data_port, uint16_t control_port,
                        const std::string& bind_address)
    : data_socket_(io_context),
      acceptor_(io_context),
      control_socket_(io_context) {
    boost::asio::ip::udp::endpoint data_endpoint = udp_bind_endpoint(bind_address, data_port);
    boost::asio::ip::tcp::endpoint control_endpoint = tcp_bind_endpoint(
        bind_address, control_port != 0 ? control_port : static_cast<uint16_t>(data_port + 1));

    // On an IPv6 socket, IPV6_V6ONLY's OS default is platform-dependent
    // (e.g. off on Linux, on on Windows). Explicitly disabling it makes
    // "::" a real dual-stack bind -- IPv4 clients arrive as IPv4-mapped
    // IPv6 addresses -- consistently across platforms.
    data_socket_.open(data_endpoint.protocol());
    if (data_endpoint.protocol() == boost::asio::ip::udp::v6()) {
        data_socket_.set_option(boost::asio::ip::v6_only(false));
    }
    data_socket_.bind(data_endpoint);

    acceptor_.open(control_endpoint.protocol());
    if (control_endpoint.protocol() == boost::asio::ip::tcp::v6()) {
        acceptor_.set_option(boost::asio::ip::v6_only(false));
    }
    acceptor_.set_option(boost::asio::socket_base::reuse_address(true));
    acceptor_.bind(control_endpoint);
    acceptor_.listen();
}

void RDTPServer::accept() {
    acceptor_.accept(control_socket_);

    std::vector<uint8_t> hdr_buf(ControlMsgHeader::wire_size);
    boost::asio::read(control_socket_, boost::asio::buffer(hdr_buf));
    ControlMsgHeader req_hdr = ControlMsgHeader::from_wire(hdr_buf.data());
    if (req_hdr.sync != RDTP_SYNC || req_hdr.msg_type != ControlMsgType::InitRequest) {
        throw std::runtime_error("RDTPServer::accept: unexpected control message from receiver");
    }
    std::vector<uint8_t> payload_buf(req_hdr.size);
    boost::asio::read(control_socket_, boost::asio::buffer(payload_buf));
    InitRequestPayload req = InitRequestPayload::from_wire(payload_buf.data());

    // On a dual-stack ("::") data_socket_, send_to() needs a v6-family
    // endpoint even for a v4 peer; map plain-v4 peer addresses to their
    // v4-mapped-v6 form so sending back to an IPv4 client works.
    boost::asio::ip::address peer_addr = control_socket_.remote_endpoint().address();
    if (data_socket_.local_endpoint().protocol() == boost::asio::ip::udp::v6() && peer_addr.is_v4()) {
        peer_addr = boost::asio::ip::make_address_v6(boost::asio::ip::v4_mapped, peer_addr.to_v4());
    }
    client_data_endpoint_ = boost::asio::ip::udp::endpoint(peer_addr, static_cast<uint16_t>(req.receiver_data_port));

    InitReplyPayload reply;
    reply.max_packet_size = max_packet_size_;
    reply.init_sequence_no = sequence_counter_;

    ControlMsgHeader reply_hdr;
    reply_hdr.msg_type = ControlMsgType::InitReply;
    reply_hdr.size = InitReplyPayload::wire_size;
    reply_hdr.cntr = control_msg_cntr_++;

    std::vector<uint8_t> out(ControlMsgHeader::wire_size + InitReplyPayload::wire_size);
    reply_hdr.to_wire(out.data());
    reply.to_wire(out.data() + ControlMsgHeader::wire_size);
    boost::asio::write(control_socket_, boost::asio::buffer(out));
}

void RDTPServer::retransmit(uint32_t seq_no) {
    // Caller must hold tracked_mutex_.
    auto it = tracked_unacked_.find(seq_no);
    if (it == tracked_unacked_.end()) return;

    const Datagram& datagram = it->second;
    std::vector<uint8_t> packet(PacketHeader::wire_size + datagram.data.size());
    datagram.header.to_wire(packet.data());
    std::copy(datagram.data.begin(), datagram.data.end(), packet.begin() + PacketHeader::wire_size);
    data_socket_.send_to(boost::asio::buffer(packet), client_data_endpoint_);
}

void RDTPServer::run_ack_loop(AckHandler on_ack) {
    while (true) {
        std::vector<uint8_t> hdr_buf(ControlMsgHeader::wire_size);
        boost::system::error_code ec;
        size_t n = boost::asio::read(control_socket_, boost::asio::buffer(hdr_buf), ec);
        if (ec || n < ControlMsgHeader::wire_size) return;  // control connection closed

        ControlMsgHeader hdr = ControlMsgHeader::from_wire(hdr_buf.data());
        if (hdr.sync != RDTP_SYNC || hdr.msg_type != ControlMsgType::Ack) {
            continue;
        }
        std::vector<uint8_t> payload_buf(hdr.size);
        boost::asio::read(control_socket_, boost::asio::buffer(payload_buf), ec);
        if (ec) return;
        AckPayload ack = AckPayload::from_wire(payload_buf.data(), payload_buf.size());

        {
            std::lock_guard<std::mutex> lock(tracked_mutex_);
            for (uint32_t seq_no : ack.missing_seq_nums) {
                retransmit(seq_no);
            }
            // Everything up to and including last_dg_acked is confirmed
            // delivered; drop it from the retransmission-tracking set.
            auto end_it = tracked_unacked_.upper_bound(ack.last_dg_acked);
            tracked_unacked_.erase(tracked_unacked_.begin(), end_it);
        }

        on_ack(ack.missing_seq_nums);
    }
}

void RDTPServer::send_datagram(Datagram datagram) {
    datagram.header.datagram_sequence = sequence_counter_++;
    datagram.header.datagram_byte_size = static_cast<uint32_t>(datagram.data.size());

    {
        std::lock_guard<std::mutex> lock(tracked_mutex_);
        // Patch in the current retransmission low-water mark: the lowest
        // sequence number still tracked as unacknowledged, or this packet's
        // own sequence number if nothing else is outstanding.
        uint32_t low_water_mark = tracked_unacked_.empty()
            ? datagram.header.datagram_sequence
            : tracked_unacked_.begin()->first;
        datagram.header.oldest_datagram_available = low_water_mark;

        tracked_unacked_[datagram.header.datagram_sequence] = datagram;
    }

    std::vector<uint8_t> packet(PacketHeader::wire_size + datagram.data.size());
    datagram.header.to_wire(packet.data());
    std::copy(datagram.data.begin(), datagram.data.end(), packet.begin() + PacketHeader::wire_size);
    data_socket_.send_to(boost::asio::buffer(packet), client_data_endpoint_);
}

void RDTPServer::stop() {
    boost::system::error_code ec;
    acceptor_.close(ec);
    data_socket_.close(ec);
    control_socket_.close(ec);
}

} // namespace rdtp
