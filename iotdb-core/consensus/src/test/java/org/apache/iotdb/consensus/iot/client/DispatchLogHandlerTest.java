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

package org.apache.iotdb.consensus.iot.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserDataTransferAuditEvent;
import org.apache.iotdb.commons.audit.UserDataTransferAuditHandler;
import org.apache.iotdb.commons.audit.UserDataTransferProtectionMethod;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.logdispatcher.Batch;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesRes;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DispatchLogHandlerTest {

  private static final TEndPoint SOURCE = new TEndPoint("127.0.0.1", 10740);
  private static final TEndPoint TARGET = new TEndPoint("127.0.0.2", 10740);

  @Test
  public void testRecordsSuccessRemoteFailureAndException() {
    final List<UserDataTransferAuditEvent> events = new ArrayList<>();
    final Batch batch = createBatch(true);

    DispatchLogHandler.recordTransferAttempt(
        events::add, batch, SOURCE, TARGET, UserDataTransferProtectionMethod.TLS, true, null, null);
    DispatchLogHandler.recordTransferAttempt(
        events::add,
        batch,
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.TLS,
        false,
        "500",
        null);
    DispatchLogHandler.recordTransferAttempt(
        events::add,
        batch,
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.TLS,
        false,
        null,
        new IOException());

    assertEquals(3, events.size());
    assertTrue(events.get(0).isSuccess());
    assertEquals(SOURCE, events.get(0).getInitiator());
    assertEquals(SOURCE, events.get(0).getSource());
    assertEquals(TARGET, events.get(0).getTarget());
    assertEquals(UserDataTransferProtectionMethod.TLS, events.get(0).getProtectionMethod());
    assertFalse(events.get(1).isSuccess());
    assertEquals("500", events.get(1).getError());
    assertEquals(IOException.class.getName(), events.get(2).getError());
  }

  @Test
  public void testSkipsBatchWithoutUserData() {
    final List<UserDataTransferAuditEvent> events = new ArrayList<>();

    DispatchLogHandler.recordTransferAttempt(
        events::add,
        createBatch(false),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        true,
        null,
        null);

    assertTrue(events.isEmpty());
  }

  @Test
  public void testSkipDoesNotInspectResponseStatuses() {
    final AtomicInteger statusAccessCount = new AtomicInteger();
    final TSyncLogEntriesRes response =
        new TSyncLogEntriesRes() {
          @Override
          public List<TSStatus> getStatuses() {
            statusAccessCount.incrementAndGet();
            return super.getStatuses();
          }
        };

    DispatchLogHandler.recordTransferAttempt(
        event -> {},
        createBatch(false),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        response);
    DispatchLogHandler.recordTransferAttempt(
        UserDataTransferAuditHandler.NO_OP,
        createBatch(true),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        response);

    assertEquals(0, statusAccessCount.get());
  }

  @Test
  public void testAuditHandlerFailureDoesNotEscape() {
    DispatchLogHandler.recordTransferAttempt(
        event -> {
          throw new IllegalStateException();
        },
        createBatch(true),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        true,
        null,
        null);

    DispatchLogHandler.recordTransferAttempt(
        new UserDataTransferAuditHandler() {
          @Override
          public void onAttempt(UserDataTransferAuditEvent event) {
            // Do nothing.
          }

          @Override
          public boolean isEnabled() {
            throw new IllegalStateException();
          }
        },
        createBatch(true),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        true,
        null,
        null);
  }

  private static Batch createBatch(boolean containsUserData) {
    final Batch batch = new Batch(IoTConsensusConfig.newBuilder().build());
    batch.addTLogEntry(new TLogEntry().setSearchIndex(1).setMemorySize(1), containsUserData);
    batch.buildIndex();
    return batch;
  }
}
