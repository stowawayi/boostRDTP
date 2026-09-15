#include "rdtp/rdtp.hpp"
#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <unordered_map>

namespace fs = std::filesystem;

struct FileReceiver {
    std::unordered_map<uint32_t, std::vector<std::vector<uint8_t>>> file_parts;
    std::unordered_map<uint32_t, uint32_t> total_segments;

    void on_data(const rdtp::Datagram& dgram) {
        auto& parts = file_parts[dgram.header.file_id];
        if (parts.empty()) parts.resize(dgram.header.segments_in_file);
        // file_segment_no is 1-based on the wire; parts is 0-indexed.
        if (dgram.header.file_segment_no == 0 || dgram.header.file_segment_no > parts.size()) {
            std::cerr << "[Client] Ignoring datagram with out-of-range file_segment_no="
                      << dgram.header.file_segment_no << std::endl;
            return;
        }
        parts[dgram.header.file_segment_no - 1] = dgram.data;
        total_segments[dgram.header.file_id] = dgram.header.segments_in_file;

        // Check if complete
        bool complete = true;
        for (const auto& segment : parts) {
            if (segment.empty()) {
                complete = false;
                break;
            }
        }

        if (complete) {
            std::ofstream out("output_file_" + std::to_string(dgram.header.file_id), std::ios::binary);
            for (const auto& part : parts) out.write((char*)part.data(), part.size());
            std::cout << "[Client] Received complete file with ID: " << dgram.header.file_id << std::endl;
        }
    }
};

int main(int argc, char* argv[]) {
    namespace po = boost::program_options;
    namespace fs_b = boost::filesystem;

    std::string output_dir, host;
    uint16_t port;
    int ack_window_size = 2048;
    int ack_window_timeout = 1000;

    po::options_description desc("RDTP Client Options");
    desc.add_options()
        ("help,h", "Show help message")
        ("host", po::value<std::string>(&host)->required(), "Server hostname or IP")
        ("port", po::value<uint16_t>(&port)->required(), "Server data port")
        ("output,o", po::value<std::string>(&output_dir)->required(), "Output folder to save files")
        ("ack-window-size", po::value<int>(&ack_window_size)->default_value(2048), "ACK window size (bytes)")
        ("ack-window-timeout", po::value<int>(&ack_window_timeout)->default_value(1000), "ACK timeout (ms)");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        if (vm.count("help")) {
            std::cout << desc << "\n";
            return 0;
        }
        po::notify(vm);
    } catch (const po::error& e) {
        std::cerr << "Error parsing CLI: " << e.what() << "\n" << desc << "\n";
        return 1;
    }

    if (!fs_b::exists(output_dir)) {
        std::cerr << "Error: Output folder does not exist.\n";
        return 1;
    }

    fs::current_path(output_dir);

    boost::asio::io_context io_context;
    rdtp::RDTPClient client(io_context, host, port);  // control channel defaults to port+1

    client.connect();
    client.send_init(static_cast<uint32_t>(ack_window_size), static_cast<uint32_t>(ack_window_timeout));

    FileReceiver receiver;
    // A real client would batch acks per ack_window_size/ack_window_timeout
    // and compute missing_seq_nums from gaps in received sequence numbers.
    client.receive_loop([&](const rdtp::Datagram& datagram) {
        receiver.on_data(datagram);
        client.send_ack({});
    });

    return 0;
}
