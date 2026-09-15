#include "rdtp/rdtp.hpp"
#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

TEST(RDTPTest, PacketHeaderDefaults) {
    rdtp::PacketHeader hdr;
    EXPECT_EQ(hdr.sync, 0xC01DBEEF);
    EXPECT_EQ(hdr.header_size, 36u);  // byte count, not a word count
}

TEST(RDTPTest, DatagramAssembly) {
    rdtp::Datagram d;
    d.header.datagram_sequence = 42;
    d.data = {1, 2, 3, 4};
    EXPECT_EQ(d.header.datagram_sequence, 42);
    EXPECT_EQ(d.data.size(), 4);
    EXPECT_EQ(d.data[0], 1);
}

// -----------------------------------------------------------------------
// Wire-format round-trip tests
// -----------------------------------------------------------------------

TEST(RDTPWireTest, PacketHeaderByteOrder) {
    rdtp::PacketHeader h;
    h.datagram_sequence = 0x01020304;
    h.data_type = 0xAABBCCDDu;
    h.oldest_datagram_available = 5;
    h.datagram_byte_size = 100;
    h.file_id = 7;
    h.file_segment_no = 3;  // 1-based on the wire
    h.segments_in_file = 10;

    uint8_t buf[rdtp::PacketHeader::wire_size];
    h.to_wire(buf);

    // sync, header_size, datagram_sequence: network byte order (big-endian).
    EXPECT_EQ(buf[0], 0xC0); EXPECT_EQ(buf[1], 0x1D); EXPECT_EQ(buf[2], 0xBE); EXPECT_EQ(buf[3], 0xEF);
    EXPECT_EQ(buf[4], 0x00); EXPECT_EQ(buf[5], 0x00); EXPECT_EQ(buf[6], 0x00); EXPECT_EQ(buf[7], 0x24);
    EXPECT_EQ(buf[8], 0x01); EXPECT_EQ(buf[9], 0x02); EXPECT_EQ(buf[10], 0x03); EXPECT_EQ(buf[11], 0x04);

    // data_type is passed through host-order, unswapped.
    uint32_t host_data_type = h.data_type;
    EXPECT_EQ(0, std::memcmp(buf + 12, &host_data_type, 4));

    rdtp::PacketHeader roundtrip = rdtp::PacketHeader::from_wire(buf);
    EXPECT_EQ(roundtrip.sync, h.sync);
    EXPECT_EQ(roundtrip.header_size, h.header_size);
    EXPECT_EQ(roundtrip.datagram_sequence, h.datagram_sequence);
    EXPECT_EQ(roundtrip.data_type, h.data_type);
    EXPECT_EQ(roundtrip.oldest_datagram_available, h.oldest_datagram_available);
    EXPECT_EQ(roundtrip.datagram_byte_size, h.datagram_byte_size);
    EXPECT_EQ(roundtrip.file_id, h.file_id);
    EXPECT_EQ(roundtrip.file_segment_no, h.file_segment_no);
    EXPECT_EQ(roundtrip.segments_in_file, h.segments_in_file);
}

TEST(RDTPWireTest, ControlMsgHeaderRoundTrip) {
    rdtp::ControlMsgHeader h;
    h.size = 12;
    h.msg_type = rdtp::ControlMsgType::Ack;
    h.cntr = 42;

    uint8_t buf[rdtp::ControlMsgHeader::wire_size];
    h.to_wire(buf);

    // msg_type == 2 (Ack), big-endian.
    EXPECT_EQ(buf[8], 0); EXPECT_EQ(buf[9], 0); EXPECT_EQ(buf[10], 0); EXPECT_EQ(buf[11], 2);

    rdtp::ControlMsgHeader roundtrip = rdtp::ControlMsgHeader::from_wire(buf);
    EXPECT_EQ(roundtrip.sync, h.sync);
    EXPECT_EQ(roundtrip.size, h.size);
    EXPECT_EQ(roundtrip.msg_type, h.msg_type);
    EXPECT_EQ(roundtrip.cntr, h.cntr);
}

TEST(RDTPWireTest, InitRequestReplyRoundTrip) {
    rdtp::InitRequestPayload req;
    req.ack_window_size = 2048;
    req.ack_window_timeout = 1000;
    req.receiver_data_port = 54321;
    uint8_t rbuf[rdtp::InitRequestPayload::wire_size];
    req.to_wire(rbuf);
    auto req_rt = rdtp::InitRequestPayload::from_wire(rbuf);
    EXPECT_EQ(req_rt.ack_window_size, req.ack_window_size);
    EXPECT_EQ(req_rt.ack_window_timeout, req.ack_window_timeout);
    EXPECT_EQ(req_rt.receiver_data_port, req.receiver_data_port);

    rdtp::InitReplyPayload reply;
    reply.max_packet_size = 4096;
    reply.init_sequence_no = 12345;
    uint8_t pbuf[rdtp::InitReplyPayload::wire_size];
    reply.to_wire(pbuf);
    auto reply_rt = rdtp::InitReplyPayload::from_wire(pbuf);
    EXPECT_EQ(reply_rt.max_packet_size, reply.max_packet_size);
    EXPECT_EQ(reply_rt.init_sequence_no, reply.init_sequence_no);
}

TEST(RDTPWireTest, AckPayloadRoundTripWithNacks) {
    rdtp::AckPayload ack;
    ack.recv_window = 2048;
    ack.last_dg_acked = 99;
    ack.missing_seq_nums = {100, 102, 105};

    std::vector<uint8_t> buf = ack.to_wire();
    EXPECT_EQ(buf.size(), 12u + 3u * 4u);

    rdtp::AckPayload roundtrip = rdtp::AckPayload::from_wire(buf.data(), buf.size());
    EXPECT_EQ(roundtrip.recv_window, ack.recv_window);
    EXPECT_EQ(roundtrip.last_dg_acked, ack.last_dg_acked);
    EXPECT_EQ(roundtrip.missing_seq_nums, ack.missing_seq_nums);
}

TEST(RDTPWireTest, AckPayloadRoundTripEmptyNacks) {
    rdtp::AckPayload ack;
    ack.recv_window = 1024;
    ack.last_dg_acked = 5;

    std::vector<uint8_t> buf = ack.to_wire();
    EXPECT_EQ(buf.size(), 12u);

    rdtp::AckPayload roundtrip = rdtp::AckPayload::from_wire(buf.data(), buf.size());
    EXPECT_TRUE(roundtrip.missing_seq_nums.empty());
    EXPECT_EQ(roundtrip.last_dg_acked, ack.last_dg_acked);
}

// -----------------------------------------------------------------------
// Malformed-header rejection: sync/header_size validation must drop the
// datagram, not throw or corrupt state. Exercised directly against
// PacketHeader::from_wire() plus the same comparison RDTPClient::receive_loop
// performs, since receive_loop itself requires a live socket.
// -----------------------------------------------------------------------

TEST(RDTPWireTest, RejectsBadSyncAndHeaderSize) {
    rdtp::PacketHeader good;
    uint8_t buf[rdtp::PacketHeader::wire_size];
    good.to_wire(buf);

    rdtp::PacketHeader parsed = rdtp::PacketHeader::from_wire(buf);
    EXPECT_EQ(parsed.sync, rdtp::RDTP_SYNC);
    EXPECT_EQ(parsed.header_size, 36u);

    // Corrupt the sync word.
    buf[0] ^= 0xFF;
    rdtp::PacketHeader bad_sync = rdtp::PacketHeader::from_wire(buf);
    EXPECT_NE(bad_sync.sync, rdtp::RDTP_SYNC);
    buf[0] ^= 0xFF;  // restore

    // Corrupt header_size.
    buf[7] = 0x09;
    rdtp::PacketHeader bad_size = rdtp::PacketHeader::from_wire(buf);
    EXPECT_NE(bad_size.header_size, 36u);
}

// -----------------------------------------------------------------------
// Localhost loopback integration test: real RDTPServer + RDTPClient over
// 127.0.0.1, exercising the TCP control-channel handshake and the UDP data
// channel end to end.
// -----------------------------------------------------------------------

TEST(RDTPIntegrationTest, LoopbackTwoSegmentFile) {
    constexpr uint16_t data_port = 39555;
    constexpr uint16_t control_port = 39556;

    boost::asio::io_context server_io;
    boost::asio::io_context client_io;

    rdtp::RDTPServer server(server_io, data_port, control_port);

    std::thread server_accept_thread([&]() {
        server.accept();
        server.run_ack_loop([](const std::vector<uint32_t>&) {});
    });

    rdtp::RDTPClient client(client_io, "127.0.0.1", data_port, control_port);
    client.connect();
    client.send_init(2048, 1000);

    std::vector<uint8_t> seg1 = {1, 2, 3, 4};
    std::vector<uint8_t> seg2 = {5, 6, 7};

    std::thread send_thread([&]() {
        rdtp::Datagram d1;
        d1.header.file_id = 1;
        d1.header.file_segment_no = 1;
        d1.header.segments_in_file = 2;
        d1.data = seg1;

        rdtp::Datagram d2;
        d2.header.file_id = 1;
        d2.header.file_segment_no = 2;
        d2.header.segments_in_file = 2;
        d2.data = seg2;

        server.send_datagram(d1);
        server.send_datagram(d2);
    });

    std::mutex received_mutex;
    std::condition_variable received_cv;
    std::vector<std::vector<uint8_t>> parts(2);
    std::atomic<int> received_count{0};

    std::thread receive_thread([&]() {
        client.receive_loop([&](const rdtp::Datagram& dgram) {
            std::lock_guard<std::mutex> lock(received_mutex);
            ASSERT_GE(dgram.header.file_segment_no, 1u);
            ASSERT_LE(dgram.header.file_segment_no, parts.size());
            parts[dgram.header.file_segment_no - 1] = dgram.data;
            ++received_count;
            received_cv.notify_all();
        });
    });

    {
        std::unique_lock<std::mutex> lock(received_mutex);
        bool ok = received_cv.wait_for(lock, std::chrono::seconds(5),
                                        [&]() { return received_count.load() >= 2; });
        ASSERT_TRUE(ok) << "Timed out waiting for both segments to arrive over the loopback";
    }

    EXPECT_EQ(parts[0], seg1);
    EXPECT_EQ(parts[1], seg2);

    send_thread.join();

    // Unblocks receive_loop()/run_ack_loop() so both threads can be joined
    // before client/server go out of scope.
    client.stop();
    server.stop();
    receive_thread.join();
    server_accept_thread.join();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
