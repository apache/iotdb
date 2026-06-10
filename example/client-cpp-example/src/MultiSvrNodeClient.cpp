/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "Session.h"
#include "SessionBuilder.h"
#include "SessionDataSet.h"

namespace {

void RunTreeExample() {
  try {
    std::vector<std::string> node_urls = {"127.0.0.1:6667", "127.0.0.1:6668",
                                          "127.0.0.1:6669"};

    auto builder = std::make_shared<SessionBuilder>();
    auto session = std::shared_ptr<Session>(builder->username("root")
                                                ->password("root")
                                                ->nodeUrls(node_urls)
                                                ->build());

    session->open();
    if (!session->checkTimeseriesExists("root.test.d1.s1")) {
      session->createTimeseries("root.test.d1.s1", TSDataType::INT64,
                                TSEncoding::RLE, CompressionType::SNAPPY);
    }
    session->deleteTimeseries("root.test.d1.s1");
    session->close();
  } catch (const std::exception &e) {
    std::cout << "Caught exception: " << e.what() << std::endl;
  }
}

// Example: continuously write/query data so you can manually stop a node
// to test client failover behavior.
void RunResilienceExample() {
  try {
    std::vector<std::string> node_urls = {"127.0.0.1:6667", "127.0.0.1:6668",
                                          "127.0.0.1:6669"};

    auto builder = std::make_shared<SessionBuilder>();
    auto session = std::shared_ptr<Session>(builder->username("root")
                                                ->password("root")
                                                ->nodeUrls(node_urls)
                                                ->build());

    session->open();

    if (!session->checkTimeseriesExists("root.resilience.d1.s1")) {
      session->createTimeseries("root.resilience.d1.s1", TSDataType::INT64,
                                TSEncoding::RLE, CompressionType::SNAPPY);
    }

    std::cout << "Starting resilience test. "
                 "Stop one node manually to see failover handling..."
              << std::endl;

    for (int i = 0; i < 60; ++i) { // run ~60 seconds
      int64_t timestamp = std::chrono::system_clock::now().time_since_epoch() /
                          std::chrono::milliseconds(1);
      std::string value = std::to_string(i);

      try {
        session->insertRecord("root.resilience.d1", timestamp, {"s1"}, {value});
        std::cout << "[Insert] ts=" << timestamp << ", value=" << value
                  << std::endl;

        auto dataset = session->executeQueryStatement(
            "SELECT s1 FROM root.resilience.d1 LIMIT 1");
        std::cout << "[Query] Got dataset: " << (dataset ? "Success" : "Null")
                  << std::endl;

      } catch (const std::exception &e) {
        std::cout << "Caught exception during resilience loop: " << e.what()
                  << std::endl;
      }

      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    session->close();
  } catch (const std::exception &e) {
    std::cout << "Caught exception in RunResilienceExample: " << e.what()
              << std::endl;
  }
}

} // namespace

int main() {
  // RunTreeExample();
  RunResilienceExample();
  return 0;
}
