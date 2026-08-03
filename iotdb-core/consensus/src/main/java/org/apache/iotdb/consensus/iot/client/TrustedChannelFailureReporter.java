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
import org.apache.iotdb.commons.audit.TrustedChannelFailureHandler;

final class TrustedChannelFailureReporter {

  private final TEndPoint initiator;
  private final TrustedChannelFailureHandler failureHandler;

  TrustedChannelFailureReporter(
      final TEndPoint initiator, final TrustedChannelFailureHandler failureHandler) {
    this.initiator = initiator;
    this.failureHandler =
        failureHandler == null ? TrustedChannelFailureHandler.NO_OP : failureHandler;
  }

  void report(final Throwable failure, final TEndPoint target) {
    if (failure == null || initiator == null || target == null) {
      return;
    }
    try {
      failureHandler.onFailure(failure, initiator, target);
    } catch (final RuntimeException reportingFailure) {
      if (reportingFailure != failure) {
        failure.addSuppressed(reportingFailure);
      }
    }
  }
}
