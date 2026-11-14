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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.consensus.raft.core.listener.ListenerEvent;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The metrics set counts the protocol events it knows and binds / unbinds without error. */
public class RaftGroupMetricsTest {

  @Test
  public void countsKnownEventsOnly() {
    TGroupStatusRes status = new TGroupStatusRes(3, 1, 2, 10, 9, 0, 12, 12);
    status.setRhoMax(4);
    RaftGroupMetrics m = new RaftGroupMetrics("TRaft", "DataRegion[1]", () -> status);
    m.onEvent(ListenerEvent.of("reject", 1).build());
    m.onEvent(ListenerEvent.of("reject", 1).build());
    m.onEvent(ListenerEvent.of("nack", 1).with("index", 3L).build());
    m.onEvent(ListenerEvent.of("ingest", 1).build()); // not counted
    assertEquals(2, m.count("reject"));
    assertEquals(1, m.count("nack"));
    assertEquals(0, m.count("ingest"));
    assertTrue(m.counts().containsKey("assert"));
    assertEquals(0, m.count("assert"));
  }

  @Test
  public void bindsAndUnbindsAgainstTheMetricService() {
    TGroupStatusRes status = new TGroupStatusRes(1, 1, 2, 0, 0, 0, 0, 0);
    RaftGroupMetrics m = new RaftGroupMetrics("Raft", "DataRegion[2]", () -> status);
    m.bindTo(MetricService.getInstance());
    m.onEvent(ListenerEvent.of("stepdown", 1).build());
    assertEquals(1, m.count("stepdown"));
    m.unbindFrom(MetricService.getInstance());
    m.onEvent(ListenerEvent.of("stepdown", 1).build()); // still counted locally after unbind
    assertEquals(2, m.count("stepdown"));
  }
}
