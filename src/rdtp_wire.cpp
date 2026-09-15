#include "rdtp/rdtp.hpp"
#include <algorithm>
#include <boost/endian/conversion.hpp>
#include <cstring>

namespace rdtp {

namespace {

inline void put_be32(uint8_t* buf, uint32_t host_value) {
    uint32_t be = boost::endian::native_to_big(host_value);
    std::memcpy(buf, &be, sizeof(be));
}

inline uint32_t get_be32(const uint8_t* buf) {
    uint32_t be;
    std::memcpy(&be, buf, sizeof(be));
    return boost::endian::big_to_native(be);
}

// data_type (PacketHeader word 3) is passed through host order, unswapped.
inline void put_raw32(uint8_t* buf, uint32_t host_value) {
    std::memcpy(buf, &host_value, sizeof(host_value));
}

inline uint32_t get_raw32(const uint8_t* buf) {
    uint32_t v;
    std::memcpy(&v, buf, sizeof(v));
    return v;
}

} // namespace

void PacketHeader::to_wire(uint8_t* buf) const {
    put_be32(buf + 0, sync);
    put_be32(buf + 4, header_size);
    put_be32(buf + 8, datagram_sequence);
    put_raw32(buf + 12, data_type);                    // not byte-swapped
    put_be32(buf + 16, oldest_datagram_available);
    put_be32(buf + 20, datagram_byte_size);
    put_be32(buf + 24, file_id);
    put_be32(buf + 28, file_segment_no);
    put_be32(buf + 32, segments_in_file);
}

PacketHeader PacketHeader::from_wire(const uint8_t* buf) {
    PacketHeader h;
    h.sync = get_be32(buf + 0);
    h.header_size = get_be32(buf + 4);
    h.datagram_sequence = get_be32(buf + 8);
    h.data_type = get_raw32(buf + 12);                  // not byte-swapped
    h.oldest_datagram_available = get_be32(buf + 16);
    h.datagram_byte_size = get_be32(buf + 20);
    h.file_id = get_be32(buf + 24);
    h.file_segment_no = get_be32(buf + 28);
    h.segments_in_file = get_be32(buf + 32);
    return h;
}

void ControlMsgHeader::to_wire(uint8_t* buf) const {
    put_be32(buf + 0, sync);
    put_be32(buf + 4, size);
    put_be32(buf + 8, static_cast<uint32_t>(msg_type));
    put_be32(buf + 12, cntr);
}

ControlMsgHeader ControlMsgHeader::from_wire(const uint8_t* buf) {
    ControlMsgHeader h;
    h.sync = get_be32(buf + 0);
    h.size = get_be32(buf + 4);
    h.msg_type = static_cast<ControlMsgType>(get_be32(buf + 8));
    h.cntr = get_be32(buf + 12);
    return h;
}

void InitRequestPayload::to_wire(uint8_t* buf) const {
    put_be32(buf + 0, ack_window_size);
    put_be32(buf + 4, ack_window_timeout);
    put_be32(buf + 8, receiver_data_port);
}

InitRequestPayload InitRequestPayload::from_wire(const uint8_t* buf) {
    InitRequestPayload p;
    p.ack_window_size = get_be32(buf + 0);
    p.ack_window_timeout = get_be32(buf + 4);
    p.receiver_data_port = get_be32(buf + 8);
    return p;
}

void InitReplyPayload::to_wire(uint8_t* buf) const {
    put_be32(buf + 0, max_packet_size);
    put_be32(buf + 4, init_sequence_no);
}

InitReplyPayload InitReplyPayload::from_wire(const uint8_t* buf) {
    InitReplyPayload p;
    p.max_packet_size = get_be32(buf + 0);
    p.init_sequence_no = get_be32(buf + 4);
    return p;
}

std::vector<uint8_t> AckPayload::to_wire() const {
    std::vector<uint8_t> buf(12 + missing_seq_nums.size() * 4);
    put_be32(buf.data() + 0, recv_window);
    put_be32(buf.data() + 4, last_dg_acked);
    put_be32(buf.data() + 8, static_cast<uint32_t>(missing_seq_nums.size()));
    for (size_t i = 0; i < missing_seq_nums.size(); ++i) {
        put_be32(buf.data() + 12 + i * 4, missing_seq_nums[i]);
    }
    return buf;
}

AckPayload AckPayload::from_wire(const uint8_t* buf, size_t len) {
    AckPayload p;
    if (len < 12) return p;
    p.recv_window = get_be32(buf + 0);
    p.last_dg_acked = get_be32(buf + 4);
    uint32_t nack_count = get_be32(buf + 8);
    size_t available = (len - 12) / 4;
    size_t count = std::min<size_t>(nack_count, available);
    p.missing_seq_nums.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        p.missing_seq_nums.push_back(get_be32(buf + 12 + i * 4));
    }
    return p;
}

} // namespace rdtp
