#include "rdtp/rdtp.hpp"
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace rdtp {

RDTPClient::RDTPClient(boost::asio::io_context& io_context, const std::string& server_ip,
                        uint16_t server_data_port, uint16_t server_control_port)
    : data_socket_(io_context, boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 0)),
      control_socket_(io_context) {
    server_data_endpoint_ = boost::asio::ip::udp::endpoint(boost::asio::ip::make_address(server_ip), server_data_port);
    uint16_t control_port = server_control_port != 0 ? server_control_port : static_cast<uint16_t>(server_data_port + 1);
    server_control_endpoint_ = boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(server_ip), control_port);
}

void RDTPClient::connect() {
    control_socket_.connect(server_control_endpoint_);
}

void RDTPClient::send_init(uint32_t ack_window_size, uint32_t ack_window_timeout) {
    ack_window_size_ = ack_window_size;

    InitRequestPayload req;
    req.ack_window_size = ack_window_size;
    req.ack_window_timeout = ack_window_timeout;
    req.receiver_data_port = data_socket_.local_endpoint().port();

    ControlMsgHeader hdr;
    hdr.msg_type = ControlMsgType::InitRequest;
    hdr.size = InitRequestPayload::wire_size;
    hdr.cntr = control_msg_cntr_++;

    std::vector<uint8_t> out(ControlMsgHeader::wire_size + InitRequestPayload::wire_size);
    hdr.to_wire(out.data());
    req.to_wire(out.data() + ControlMsgHeader::wire_size);
    boost::asio::write(control_socket_, boost::asio::buffer(out));

    // Block for the InitReply.
    std::vector<uint8_t> hdr_buf(ControlMsgHeader::wire_size);
    boost::asio::read(control_socket_, boost::asio::buffer(hdr_buf));
    ControlMsgHeader reply_hdr = ControlMsgHeader::from_wire(hdr_buf.data());
    if (reply_hdr.sync != RDTP_SYNC || reply_hdr.msg_type != ControlMsgType::InitReply) {
        throw std::runtime_error("RDTPClient::send_init: unexpected control message from sender");
    }
    std::vector<uint8_t> payload_buf(reply_hdr.size);
    boost::asio::read(control_socket_, boost::asio::buffer(payload_buf));
    InitReplyPayload reply = InitReplyPayload::from_wire(payload_buf.data());

    init_sequence_no_ = reply.init_sequence_no;
    next_expected_seq_ = reply.init_sequence_no;
}

void RDTPClient::receive_loop(DataHandler on_data) {
    std::vector<uint8_t> buffer(65536);
    boost::asio::ip::udp::endpoint sender_endpoint;
    while (true) {
        boost::system::error_code ec;
        size_t bytes_received = data_socket_.receive_from(boost::asio::buffer(buffer), sender_endpoint, 0, ec);
        if (ec) {
            return;  // socket closed (e.g. via stop()) or another transport error
        }
        if (bytes_received < PacketHeader::wire_size) {
            continue;
        }

        PacketHeader header = PacketHeader::from_wire(buffer.data());
        if (header.sync != RDTP_SYNC || header.header_size != PacketHeader::wire_size) {
            ++corrupted_packet_count_;
            continue;
        }

        Datagram datagram;
        datagram.header = header;
        datagram.data.assign(buffer.begin() + PacketHeader::wire_size, buffer.begin() + bytes_received);
        received_packets_[datagram.header.datagram_sequence] = datagram;

        while (received_packets_.count(next_expected_seq_)) {
            ++next_expected_seq_;
        }

        on_data(datagram);
    }
}

void RDTPClient::send_ack(const std::vector<uint32_t>& missing_seq_nums) {
    AckPayload payload;
    payload.recv_window = ack_window_size_;
    payload.last_dg_acked = next_expected_seq_ - 1;  // highest contiguously-received sequence number
    payload.missing_seq_nums = missing_seq_nums;

    std::vector<uint8_t> payload_bytes = payload.to_wire();

    ControlMsgHeader hdr;
    hdr.msg_type = ControlMsgType::Ack;
    hdr.size = static_cast<uint32_t>(payload_bytes.size());
    hdr.cntr = control_msg_cntr_++;

    std::vector<uint8_t> out(ControlMsgHeader::wire_size + payload_bytes.size());
    hdr.to_wire(out.data());
    std::copy(payload_bytes.begin(), payload_bytes.end(), out.begin() + ControlMsgHeader::wire_size);

    boost::asio::write(control_socket_, boost::asio::buffer(out));
}

void RDTPClient::stop() {
    boost::system::error_code ec;
    data_socket_.close(ec);
    control_socket_.close(ec);
}

} // namespace rdtp
