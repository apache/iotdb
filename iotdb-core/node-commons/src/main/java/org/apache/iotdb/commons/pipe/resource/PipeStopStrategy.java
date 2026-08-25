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

package org.apache.iotdb.commons.pipe.resource;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkResourceException;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.annotation.Nullable;

public final class PipeStopStrategy {

  private PipeStopStrategy() {}

  /**
   * @return {@code true} if the failure may follow the normal stop/report path, or {@code false} if
   *     it is a transient resource failure that must only be retried locally
   */
  public static boolean accept(
      final @Nullable Exception exception, final @Nullable TSStatus status) {
    return getResourceFailureType(exception, status) == null;
  }

  public static PipeResourceFailureType getResourceFailureType(
      final @Nullable Exception exception, final @Nullable TSStatus status) {
    Throwable current = exception;
    while (current != null) {
      if (current instanceof PipeRuntimeSinkResourceException) {
        return ((PipeRuntimeSinkResourceException) current).getFailureType();
      }
      if (current instanceof PipeRuntimeOutOfMemoryCriticalException) {
        return PipeResourceFailureType.MEMORY_TIMEOUT;
      }
      if (current instanceof ClientManagerException) {
        return PipeResourceFailureType.NETWORK_TIMEOUT;
      }
      current = current.getCause();
    }

    return status != null
            && status.getCode()
                == TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()
        ? PipeResourceFailureType.RECEIVER_UNAVAILABLE
        : null;
  }

  public static boolean isResourceFailureRecorded(final @Nullable Exception exception) {
    Throwable current = exception;
    while (current != null) {
      if (current instanceof PipeRuntimeSinkResourceException) {
        return ((PipeRuntimeSinkResourceException) current).isFailureRecorded();
      }
      current = current.getCause();
    }
    return false;
  }
}
