# RDTP - Reliable Data Transfer Protocol

RDTP is a reliable UDP-based file transfer library and command-line tool suite, inspired by the techniques described in [US Patent US6831912B1](https://patents.google.com/patent/US6831912B1/), now in the public domain.

## Features
- Cross-platform C++17 using Boost.Asio
- Efficient support for high-latency, asymmetric links
- File segmentation and reassembly
- Negative-ACK retransmission
- MIT licensed

## Requirements
- CMake 3.16+
- Boost (system, filesystem, program_options)
- GoogleTest (for unit tests)

## Build Instructions
```bash
mkdir build && cd build
cmake ..
make
```

## Usage
### Server
```bash
./rdtp_server \
    --folder /path/to/send \
    [--ack-window-size 2048] \
    [--ack-window-timeout 1000]
```

### Client
```bash
./rdtp_client \
    --output /path/to/save \
    [--ack-window-size 2048] \
    [--ack-window-timeout 1000]
```

### Parameters
| Flag | Description | Default |
|------|-------------|---------|
| `--ack-window-size` | Bytes to receive before sending ACK | `2048` |
| `--ack-window-timeout` | Milliseconds before ACK is forced | `1000` |

## Test
```bash
cd build
ctest
```

## License
MIT. See [LICENSE](LICENSE) file for more details.

---
This implementation is inspired by concepts from US Patent US6831912B1,
originally invented by Jon H. Sherman and assigned to Raytheon Company.
The patent has expired and is in the public domain.
