#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <random>
#include <ctime>
#include <map>

namespace Hashing {
    // generate random string
    std::string generateRandomString(int length) {
        std::random_device rd;
        std::mt19937 rng(rd()); // Seed the random number generator with current time
        std::uniform_int_distribution<int> distribution('a', 'z'); // ASCII values for lowercase letters

        std::string randomString;
        randomString.reserve(length);

        for (int i = 0; i < length; ++i) {
            randomString.push_back(static_cast<char>(distribution(rng)));
        }
        return randomString;
    }

    uint64_t constexpr INVALID_ID = 0xFFFFFFFFFFFFFFFF;

    class VirtualServer;


    using RingDomain = uint64_t; // what type of variable is used to index the ring?
    using RingElemType = VirtualServer const *; // each ring slot stores a pointer to a Virtual Server
    using Ring = std::map<RingDomain, RingElemType>;

    class Server {
        std::string hostname;
        uint64_t id = INVALID_ID;
        std::vector<VirtualServer> virtualServers;

    public:
        Server(std::string_view const hostname, uint64_t const id, std::size_t numVirtualServers) : hostname(hostname),
                                                                                                    id(id) {
            for (uint64_t i = 0; i < numVirtualServers; i++) {
                virtualServers.emplace_back(i, this);
            }
        }

        [[nodiscard]] auto get_id() const {
            return id;
        }

        [[nodiscard]] auto get_hostname() const {
            return hostname;
        }

        [[nodiscard]] auto begin() const {
            return virtualServers.cbegin();
        }

        [[nodiscard]] auto end() const {
            return virtualServers.cend();
        }
    };

    class VirtualServer {
        uint64_t id = INVALID_ID;
        Server const *const owner;

    public:
        VirtualServer(uint64_t const id, Server const *const owner) : id(id), owner(owner) {
        }

        [[nodiscard]] auto get_owner() const {
            return owner;
        }

        [[nodiscard]] auto get_id() const {
            return id;
        }
    };
    using Request = uint64_t;
    class consistent_hasher {
        static uint16_t constexpr numSlots = 512;
        static uint64_t constexpr numVirtualServers = 9;
        uint64_t nextServerId = 0;
        // hostname -> server mapping
        std::unordered_map<std::string, std::unique_ptr<Server>> servers;

        Ring ring; // fixed vector

        static RingDomain hash(VirtualServer const &virtualServer) {
            auto const i = virtualServer.get_owner()->get_id();
            auto const j = virtualServer.get_id();
            return (i * i + j * j + 2 * j + 25) % numSlots;
        }

        static RingDomain hash(Request i) {
            return (i * i + 2 * i + 17) % numSlots;
        }

        void add_server(std::string suggested_hostname) {
            // check if suggested hostname is already in use
            while (servers.contains(suggested_hostname)) {
                // generate a random hostname
                suggested_hostname = generateRandomString(10);
            }
            servers[suggested_hostname] = std::make_unique<Server>(suggested_hostname, nextServerId++,
                                                                   numVirtualServers);
            for (auto &virtualServer: *servers[suggested_hostname]) {
                // try to insert virtual server into ring
                auto hashValue = hash(virtualServer);
                bool inserted = false;

                // linear probing
                for (uint64_t i = 0; i < numSlots; i++) {
                    auto index = hashValue + 11 * i;
                    if (ring.contains(index)) continue;
                    else {
                        ring[index] = &virtualServer;
                        inserted = true;
                        break;
                    }
                }
                if (!inserted) {
                    // all possible slots are filled, need to error
                }
            }
        }

        [[nodiscard]] Server const &get_server(Request const &req) const {
            if(ring.empty()){
                // need to error out
            }
            auto index = hash(req);
            // check the ring starting at index, and using binary search, check for the immediately succeeding
            // virtual server
            auto it = ring.lower_bound(index);
            if(it == ring.end()){
                // there's no virtual server with pos >= index and pos < numSlots.
                // We need to pick the first virtual server with pos >= 0
                it = ring.begin();
            }
            auto const &virtualServer = it->second;
            return *virtualServer->get_owner();
        }
    };
}
