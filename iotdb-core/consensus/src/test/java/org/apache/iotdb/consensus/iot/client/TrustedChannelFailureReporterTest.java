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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TrustedChannelFailureReporterTest {

  @Test
  public void testReportIncludesAccurateEndpoints() {
    final TEndPoint initiator = new TEndPoint("127.0.0.1", 6667);
    final TEndPoint target = new TEndPoint("127.0.0.2", 6668);
    final RuntimeException failure = new RuntimeException();
    final AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
    final AtomicReference<TEndPoint> reportedInitiator = new AtomicReference<>();
    final AtomicReference<TEndPoint> reportedTarget = new AtomicReference<>();

    new TrustedChannelFailureReporter(
            initiator,
            (throwable, source, destination) -> {
              reportedFailure.set(throwable);
              reportedInitiator.set(source);
              reportedTarget.set(destination);
            })
        .report(failure, target);

    assertSame(failure, reportedFailure.get());
    assertSame(initiator, reportedInitiator.get());
    assertSame(target, reportedTarget.get());
  }

  @Test
  public void testReportingFailureDoesNotHideOriginalFailure() {
    final TEndPoint initiator = new TEndPoint("127.0.0.1", 6667);
    final TEndPoint target = new TEndPoint("127.0.0.2", 6668);
    final RuntimeException failure = new RuntimeException();
    final RuntimeException reportingFailure = new RuntimeException();

    new TrustedChannelFailureReporter(
            initiator,
            (throwable, source, destination) -> {
              throw reportingFailure;
            })
        .report(failure, target);

    assertEquals(1, failure.getSuppressed().length);
    assertSame(reportingFailure, failure.getSuppressed()[0]);
  }
}
