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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public final class DataNodeTransportFailureUtils {

  private static final int TRANSPORT_FAILURE_STATUS_CODE =
      TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode();

  private DataNodeTransportFailureUtils() {}

  public static boolean isTransportException(final Exception exception) {
    final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = exception;
        cause != null && visited.add(cause);
        cause = cause.getCause()) {
      if (cause instanceof TApplicationException || cause instanceof TProtocolException) {
        return false;
      }
      if (cause instanceof TTransportException) {
        final int type = ((TTransportException) cause).getType();
        // Corrupt frames and opening an already-open transport are not connection failures.
        return type == TTransportException.UNKNOWN
            || type == TTransportException.NOT_OPEN
            || type == TTransportException.TIMED_OUT
            || type == TTransportException.END_OF_FILE;
      }
      if (cause instanceof SocketException
          || cause instanceof SocketTimeoutException
          || cause instanceof ClosedChannelException
          || cause instanceof TimeoutException) {
        return true;
      }
    }
    return false;
  }

  public static TSStatus markTransportFailure(final TSStatus status) {
    // CN-local provenance: preserve the metadata error code used by existing callers.
    final List<TSStatus> subStatus = new ArrayList<>();
    if (status.isSetSubStatus() && status.getSubStatus() != null) {
      subStatus.addAll(status.getSubStatus());
    }
    subStatus.add(new TSStatus(TRANSPORT_FAILURE_STATUS_CODE));
    status.setSubStatus(subStatus);
    return status;
  }

  public static boolean isTransportFailure(final TSStatus status) {
    return status != null
        && status.isSetSubStatus()
        && status.getSubStatus() != null
        && status.getSubStatus().stream()
            .anyMatch(
                subStatus ->
                    subStatus != null && subStatus.getCode() == TRANSPORT_FAILURE_STATUS_CODE);
  }
}
