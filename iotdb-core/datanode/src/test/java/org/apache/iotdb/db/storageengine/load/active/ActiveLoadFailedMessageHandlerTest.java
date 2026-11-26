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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ActiveLoadFailedMessageHandlerTest {

  @Test
  public void testProcedureTimeoutShouldRetryInBothLocales() {
    final ActiveLoadPendingQueue.ActiveLoadEntry entry =
        new ActiveLoadPendingQueue.ActiveLoadEntry("test.tsfile", "pending", true, false);
    for (final String message :
        Arrays.asList(
            "Timed out to wait for procedure return. The procedure is still running.",
            "等待流程返回超时，流程仍在运行中。")) {
      Assert.assertTrue(
          message,
          ActiveLoadFailedMessageHandler.isStatusShouldRetry(
              entry,
              new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(message)));
    }
  }

  @Test
  public void testTemporaryUnavailableStatusShouldRetryWithoutMessageMatch() {
    final ActiveLoadPendingQueue.ActiveLoadEntry entry =
        new ActiveLoadPendingQueue.ActiveLoadEntry("test.tsfile", "pending", true, false);

    Assert.assertTrue(
        ActiveLoadFailedMessageHandler.isStatusShouldRetry(
            entry,
            new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
                .setMessage("temporarily unavailable")));
  }

  @Test
  public void testPermanentStatusShouldNotRetryWithoutMessageMatch() {
    final ActiveLoadPendingQueue.ActiveLoadEntry entry =
        new ActiveLoadPendingQueue.ActiveLoadEntry("test.tsfile", "pending", true, false);

    Assert.assertFalse(
        ActiveLoadFailedMessageHandler.isStatusShouldRetry(
            entry, new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage("bad")));
  }
}
