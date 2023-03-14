/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.metricsets.net;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public interface INetMetricManager {
  static INetMetricManager getNetMetricManager() {
    String os = System.getProperty("os.name").toLowerCase();

    if (os.startsWith("windows")) {
      return new WindowsNetMetricManager();
    } else if (os.startsWith("linux")) {
      return new LinuxNetMetricManager();
    } else {
      return new MacNetMetricManager();
    }
  }

  default Map<String, Long> getReceivedByte() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getTransmittedBytes() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getReceivedPackets() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getTransmittedPackets() {
    return Collections.emptyMap();
  }

  default Set<String> getIfaceSet() {
    return Collections.emptySet();
  }
}
