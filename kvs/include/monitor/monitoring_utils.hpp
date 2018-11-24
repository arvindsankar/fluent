//  Copyright 2018 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#ifndef SRC_INCLUDE_MONITOR_MONITORING_UTILS_HPP_
#define SRC_INCLUDE_MONITOR_MONITORING_UTILS_HPP_

#include "hash_ring.hpp"
#include "requests.hpp"
#include "spdlog/spdlog.h"

// define monitoring threshold (in second)
const unsigned kMonitoringThreshold = 30;

// define the grace period for triggering elasticity action (in second)
const unsigned kGracePeriod = 120;

// the default number of nodes to add concurrently for storage
const unsigned kNodeAdditionBatchSize = 2;

// define capacity for both tiers
const double kMaxMemoryNodeConsumption = 0.6;
const double kMinMemoryNodeConsumption = 0.3;
const double kMaxEbsNodeConsumption = 0.75;
const double kMinEbsNodeConsumption = 0.5;

// define threshold for promotion/demotion
const unsigned kKeyPromotionThreshold = 0;
const unsigned kKeyDemotionThreshold = 1;

// define minimum number of nodes for each tier
const unsigned kMinMemoryTierSize = 1;
const unsigned kMinEbsTierSize = 0;

// value size in KB
const unsigned kValueSize = 256;

struct SummaryStats {
  void clear() {
    key_access_cnt = 0;
    key_access_mean = 0;
    key_access_std = 0;
    total_memory_count = 0;
    total_memory_access = 0;
    total_ebs_count = 0;
    total_ebs_access = 0;
    total_memory_consumption = 0;
    total_ebs_consumption = 0;
    max_memory_consumption_percentage = 0;
    max_ebs_consumption_percentage = 0;
    avg_memory_consumption_percentage = 0;
    avg_ebs_consumption_percentage = 0;
    required_memory_node = 0;
    required_ebs_node = 0;
    max_memory_occupancy = 0;
    min_memory_occupancy = 1;
    avg_memory_occupancy = 0;
    sum_memory_occupancy = 0;
    cnt_memory_occupancy = 0;
    max_ebs_occupancy = 0;
    min_ebs_occupancy = 1;
    avg_ebs_occupancy = 0;
    sum_ebs_occupancy = 0;
    cnt_ebs_occupancy = 0;
    min_occupancy_memory_public_ip = Address();
    min_occupancy_memory_private_ip = Address();
    sum_latency = 0;
    cnt_latency = 0;
    avg_latency = 0;
    total_throughput = 0;
  }
  void aggregate(SummaryStats ss) {
    double total = key_access_cnt*key_access_mean + ss.key_access_cnt*ss.key_access_mean;
    key_access_cnt = key_access_cnt + ss.key_access_cnt;
    if (key_access_cnt != 0) {
      key_access_mean = total / key_access_cnt;
    } else {
      key_access_mean = 0;
    }
    key_access_std = sqrt(key_access_std*key_access_std + ss.key_access_std*ss.key_access_std);

    total_memory_count += ss.total_memory_count;
    total_memory_access += ss.total_memory_access;
    total_ebs_count += ss.total_ebs_count;
    total_ebs_access += ss.total_ebs_access;
    total_memory_consumption += ss.total_memory_consumption;
    total_ebs_consumption += ss.total_ebs_consumption;

    max_memory_consumption_percentage = fmax(max_memory_consumption_percentage, ss.max_memory_consumption_percentage);
    max_ebs_consumption_percentage = fmax(max_ebs_consumption_percentage, ss.max_ebs_consumption_percentage);

    if (total_memory_count != 0) {
      avg_memory_consumption_percentage = total_memory_consumption / total_memory_count;
    } else {
      avg_memory_consumption_percentage = 0; 
    }
    if (total_ebs_count != 0) {
      avg_ebs_consumption_percentage = total_ebs_consumption / total_ebs_count;
    } else {
      avg_ebs_consumption_percentage = 0;
    }
    
    memory_node_capacity = ss.memory_node_capacity;
    ebs_node_capacity = ss.ebs_node_capacity;

    if (memory_node_capacity != 0) {
      required_memory_node = ceil(total_memory_consumption /
          (kMaxMemoryNodeConsumption * memory_node_capacity));
    } else {
      required_memory_node = 0;
    }

    if (ebs_node_capacity != 0) {
      required_ebs_node = ceil(total_ebs_consumption /
          (kMaxMemoryNodeConsumption * ebs_node_capacity));
    } else {
      required_ebs_node = 0;
    }

    if (ss.min_memory_occupancy < min_memory_occupancy) {
      min_occupancy_memory_public_ip = ss.min_occupancy_memory_public_ip;
      min_occupancy_memory_private_ip = ss.min_occupancy_memory_private_ip;
    }

    max_memory_occupancy = fmax(max_memory_occupancy, ss.max_memory_occupancy);
    min_memory_occupancy = fmin(min_memory_occupancy, ss.min_memory_occupancy);
    sum_memory_occupancy += ss.sum_memory_occupancy;
    cnt_memory_occupancy += ss.cnt_memory_occupancy;

    if (cnt_memory_occupancy != 0) {
      avg_memory_occupancy = sum_memory_occupancy / cnt_memory_occupancy;
    } else {
      avg_memory_occupancy = 0;
    }
    
    max_ebs_occupancy = fmax(max_ebs_occupancy, ss.max_ebs_occupancy);
    min_ebs_occupancy = fmin(min_ebs_occupancy, ss.min_ebs_occupancy);
    sum_ebs_occupancy += ss.sum_ebs_occupancy;
    cnt_ebs_occupancy += ss.cnt_ebs_occupancy;

    if (cnt_ebs_occupancy != 0) {
      avg_ebs_occupancy = sum_ebs_occupancy / cnt_ebs_occupancy;
    } else {
      avg_ebs_occupancy = 0;
    }

    sum_latency += ss.sum_latency;
    cnt_latency += ss.cnt_latency;
    if (cnt_latency != 0) {
      avg_latency = sum_latency / cnt_latency;
    } else {
      avg_latency = 0;
    }

    total_throughput += ss.total_throughput;
  }
  SummaryStats() { clear(); }
  unsigned key_access_cnt;
  double key_access_mean;
  double key_access_std;
  unsigned total_memory_count;
  unsigned total_memory_access;
  unsigned total_ebs_count;
  unsigned total_ebs_access;
  unsigned long long total_memory_consumption;
  unsigned long long total_ebs_consumption;
  double max_memory_consumption_percentage;
  double max_ebs_consumption_percentage;
  double avg_memory_consumption_percentage;
  double avg_ebs_consumption_percentage;
  double memory_node_capacity;
  double ebs_node_capacity;
  unsigned required_memory_node;
  unsigned required_ebs_node;
  double max_memory_occupancy;
  double min_memory_occupancy;
  double avg_memory_occupancy;
  double sum_memory_occupancy;
  unsigned cnt_memory_occupancy;
  double max_ebs_occupancy;
  double min_ebs_occupancy;
  double avg_ebs_occupancy;
  double sum_ebs_occupancy;
  unsigned cnt_ebs_occupancy;
  Address min_occupancy_memory_public_ip;
  Address min_occupancy_memory_private_ip;
  double sum_latency;
  unsigned cnt_latency;
  double avg_latency;
  double total_throughput;
};

struct thread_stats {
  // keep track of the keys' access by worker address
  std::unordered_map<Key, std::unordered_map<Address, unsigned>>
      key_access_frequency;
  // keep track of the keys' access summary
  std::unordered_map<Key, unsigned> key_access_summary;
  // keep track of the size of each key-value pair
  std::unordered_map<Key, unsigned> key_size;
  // keep track of memory tier storage consumption
  StorageStat memory_tier_storage;
  // keep track of ebs tier storage consumption
  StorageStat ebs_tier_storage;
  // keep track of memory tier thread occupancy
  OccupancyStats memory_tier_occupancy;
  // keep track of ebs tier thread occupancy
  OccupancyStats ebs_tier_occupancy;
  // keep track of memory tier hit
  AccessStat memory_tier_access;
  // keep track of ebs tier hit
  AccessStat ebs_tier_access;
  // keep track of some summary statistics
  SummaryStats ss;
  // keep track of user latency info
  std::unordered_map<std::string, double> user_latency;
  // keep track of user throughput info
  std::unordered_map<std::string, double> user_throughput;
  // used for adjusting the replication factors based on feedback from the user
  std::unordered_map<Key, std::pair<double, unsigned>> latency_miss_ratio_map;
};

Address prepare_metadata_request(
    const Key& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    std::unordered_map<Address, KeyRequest>& addr_request_map,
    MonitoringThread& mt, unsigned& rid, std::string type);

void prepare_metadata_get_request(
    const Key& key, GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    std::unordered_map<Address, KeyRequest>& addr_request_map,
    MonitoringThread& mt, unsigned& rid);

void prepare_metadata_put_request(
    const Key& key, const std::string& value,
    GlobalHashRing& global_memory_hash_ring,
    LocalHashRing& local_memory_hash_ring,
    std::unordered_map<Address, KeyRequest>& addr_request_map,
    MonitoringThread& mt, unsigned& rid);

void get_key_responses(std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map, std::vector<KeyResponse>& responses, 
    SocketCache& pushers, MonitoringThread& mt,
    zmq::socket_t& response_puller, unsigned& rid);

void collect_internal_stats(std::vector<KeyResponse> responses,
    std::shared_ptr<spdlog::logger> logger, unsigned& rid, 
    unsigned thread_count, unsigned tid, thread_stats& ts);

void compute_summary_stats(thread_stats& ts,
    std::shared_ptr<spdlog::logger> logger, unsigned& server_monitoring_epoch);

void collect_external_stats(thread_stats& ts, std::shared_ptr<spdlog::logger> logger);

KeyInfo create_new_replication_vector(unsigned gm, unsigned ge, unsigned lm,
                                      unsigned le);

void prepare_replication_factor_update(
    const Key& key,
    std::unordered_map<Address, ReplicationFactorUpdate>&
        replication_factor_map,
    Address server_address, std::unordered_map<Key, KeyInfo>& placement);

void change_replication_factor(
    std::unordered_map<Key, KeyInfo>& requests,
    std::unordered_map<unsigned, GlobalHashRing>& global_hash_ring_map,
    std::unordered_map<unsigned, LocalHashRing>& local_hash_ring_map,
    std::vector<Address>& routing_address,
    std::unordered_map<Key, KeyInfo>& placement, SocketCache& pushers,
    MonitoringThread& mt, zmq::socket_t& response_puller,
    std::shared_ptr<spdlog::logger> logger, unsigned& rid);

void add_node(std::shared_ptr<spdlog::logger> logger, std::string tier,
              unsigned number, unsigned& adding, SocketCache& pushers,
              const Address& management_address);

void remove_node(std::shared_ptr<spdlog::logger> logger, ServerThread& node,
                 std::string tier, bool& removing_flag, SocketCache& pushers,
                 std::unordered_map<Address, unsigned>& departing_node_map,
                 MonitoringThread& mt);

#endif  // SRC_INCLUDE_MONITOR_MONITORING_UTILS_HPP_
