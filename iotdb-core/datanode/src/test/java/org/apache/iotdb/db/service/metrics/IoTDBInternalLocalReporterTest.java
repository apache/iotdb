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

package org.apache.iotdb.db.service.metrics;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IoTDBInternalLocalReporterTest {

  @Test
  public void sameFailureLoggedOnlyAfterIntervalOrRecovery() {
    IoTDBInternalLocalReporter.FailureLogState failureLogState =
        new IoTDBInternalLocalReporter.FailureLogState();
    long startTime = 1000L;

    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(failureLogState, "failure-status", startTime));
    Assert.assertFalse(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogState, "failure-status", startTime + 1));

    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogState,
            "failure-status",
            startTime + IoTDBInternalLocalReporter.FAILURE_LOG_INTERVAL));
    Assert.assertFalse(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogState,
            "failure-status",
            startTime + IoTDBInternalLocalReporter.FAILURE_LOG_INTERVAL + 1));

    IoTDBInternalLocalReporter.clearFailureLogState(failureLogState);
    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogState, "failure-status", startTime + 2));
  }

  @Test
  public void differentFailuresLoggedImmediately() {
    IoTDBInternalLocalReporter.FailureLogState failureLogState =
        new IoTDBInternalLocalReporter.FailureLogState();
    long startTime = 1000L;

    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogState, "failure-status-1", startTime));
    Assert.assertFalse(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogState, "failure-status-1", startTime + 1));
    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogState, "failure-status-2", startTime + 2));
  }

  @Test
  public void failureLogStateIsIsolatedByMetricPrefix() {
    Map<String, IoTDBInternalLocalReporter.FailureLogState> failureLogStateMap =
        new ConcurrentHashMap<>();
    long startTime = 1000L;

    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogStateMap, "root.__system.metric1", "failure-status", startTime));
    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogStateMap, "root.__system.metric2", "failure-status", startTime + 1));
    Assert.assertFalse(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogStateMap, "root.__system.metric1", "failure-status", startTime + 2));

    IoTDBInternalLocalReporter.clearFailureLogState(failureLogStateMap, "root.__system.metric2");
    Assert.assertTrue(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogStateMap, "root.__system.metric2", "failure-status", startTime + 3));
    Assert.assertFalse(
        IoTDBInternalLocalReporter.shouldLogFailure(
            failureLogStateMap, "root.__system.metric1", "failure-status", startTime + 4));
  }
}
