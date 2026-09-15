#include "rdtp/rdtp.hpp"
#include <algorithm>
#include <boost/asio.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
    if (argc != 2 && argc != 3) {
        std::cerr << "Usage: rdtp_server <folder> [bind-address]\n"
                      "  bind-address defaults to :: (dual-stack IPv4+IPv6); pass an IPv4 address to bind IPv4 only.\n";
        return 1;
    }

    fs::path folder = argv[1];
    if (!fs::exists(folder) || !fs::is_directory(folder)) {
        std::cerr << "Invalid folder." << std::endl;
        return 1;
    }
    std::string bind_address = argc == 3 ? argv[2] : "::";

    boost::asio::io_context io_context;
    rdtp::RDTPServer server(io_context, 9000, 0, bind_address);  // control channel defaults to port 9001

    std::cout << "[Server] Waiting for receiver to connect..." << std::endl;
    server.accept();
    std::cout << "[Server] Receiver connected and initialized." << std::endl;

    // Runs concurrently with sending so retransmissions can happen while
    // new segments are still going out.
    std::thread ack_thread([&server]() {
        server.run_ack_loop([](const std::vector<uint32_t>& missing_seq_nums) {
            if (!missing_seq_nums.empty()) {
                std::cout << "[Server] Ack received, " << missing_seq_nums.size()
                          << " segment(s) retransmitted." << std::endl;
            }
        });
    });

    uint32_t file_id = 1;
    for (const auto& file : fs::directory_iterator(folder)) {
        if (!file.is_regular_file()) continue;

        std::ifstream in(file.path(), std::ios::binary);
        std::vector<uint8_t> content((std::istreambuf_iterator<char>(in)), {});
        uint32_t max_segment_bytes = std::min<uint32_t>(server.max_packet_size(), 1024);
        size_t total_segments = (content.size() + max_segment_bytes - 1) / max_segment_bytes;

        for (size_t i = 0; i < total_segments; ++i) {
            rdtp::Datagram dgram;
            dgram.header.data_type = 0;
            dgram.header.file_id = file_id;
            dgram.header.file_segment_no = static_cast<uint32_t>(i) + 1;  // 1-based
            dgram.header.segments_in_file = static_cast<uint32_t>(total_segments);
            size_t chunk_size = std::min<size_t>(max_segment_bytes, content.size() - i * max_segment_bytes);
            dgram.data.insert(dgram.data.end(),
                               content.begin() + i * max_segment_bytes,
                               content.begin() + i * max_segment_bytes + chunk_size);

            server.send_datagram(std::move(dgram));
        }

        std::cout << "[Server] Sent file with ID: " << file_id << std::endl;
        ++file_id;
    }

    ack_thread.join();

    return 0;
}
