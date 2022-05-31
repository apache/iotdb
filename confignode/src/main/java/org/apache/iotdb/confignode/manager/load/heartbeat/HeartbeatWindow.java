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
package org.apache.iotdb.confignode.manager.load.heartbeat;

import java.util.LinkedList;

/**
 * HeartbeatWindow contains Heartbeat's sending and receiving time, which is used for estimating
 * when the next heartbeat will arrive.
 */
public class HeartbeatWindow {

  private static final int maximumWindowSize = 1000;

  private final LinkedList<HeartbeatPackage> slidingWindow;

  public HeartbeatWindow() {
    this.slidingWindow = new LinkedList<>();
  }

  public void addHeartbeat(HeartbeatPackage newHeartbeat) {
    synchronized (slidingWindow) {
      // Only sequential heartbeats are accepted.
      // And un-sequential heartbeats will be discarded.
      if (slidingWindow.size() == 0
          || slidingWindow.getLast().getSendTimestamp() < newHeartbeat.getSendTimestamp()) {
        slidingWindow.add(newHeartbeat);
      }

      while (slidingWindow.size() > maximumWindowSize) {
        slidingWindow.removeFirst();
      }
    }
  }
}
