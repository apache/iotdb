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
package org.apache.iotdb.db.sync;

import org.junit.Assert;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

/** Simulate network delay and loss. */
public class SyncWeakNetworkIT extends SyncIT {
  @Override
  public void init() throws Exception {
    super.init();
    // set delay is 200±50ms that conform to a normal distribution;
    // network packet with 10% loss rate, 10% duplicate rate, 10% reorder rate and 10% corrupt rate;
    Container.ExecResult res =
        ((ContainerState) environment.getContainerByServiceName("iotdb-sender_1").get())
            .execInContainer(
                "sh",
                "-c",
                "tc qdisc add dev eth0 root netem delay 200ms 50ms 25% distribution normal loss random 10% duplicate 10% reorder 10% corrupt 10%");
    Assert.assertEquals(0, res.getExitCode());
  }
}
