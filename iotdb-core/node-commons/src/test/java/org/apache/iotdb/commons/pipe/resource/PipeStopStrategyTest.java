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
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PipeStopStrategyTest {

  @Test
  public void testClientBorrowFailureDoesNotStopPipe() {
    final Exception failure =
        new PipeConnectionException(
            "transfer failed",
            new IOException(new ClientManagerException(new IOException("client pool exhausted"))));

    Assert.assertFalse(PipeStopStrategy.accept(failure, null));
    Assert.assertEquals(
        PipeResourceFailureType.NETWORK_TIMEOUT,
        PipeStopStrategy.getResourceFailureType(failure, null));
  }

  @Test
  public void testMemoryFailuresDoNotStopPipe() {
    final PipeRuntimeOutOfMemoryCriticalException exception =
        new PipeRuntimeOutOfMemoryCriticalException("memory unavailable");
    Assert.assertFalse(PipeStopStrategy.accept(exception, null));
    Assert.assertEquals(
        PipeResourceFailureType.MEMORY_TIMEOUT,
        PipeStopStrategy.getResourceFailureType(exception, null));

    final TSStatus status =
        new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode());
    Assert.assertFalse(PipeStopStrategy.accept(null, status));
    Assert.assertEquals(
        PipeResourceFailureType.RECEIVER_UNAVAILABLE,
        PipeStopStrategy.getResourceFailureType(null, status));
  }

  @Test
  public void testOtherFailuresKeepExistingStopPolicy() {
    Assert.assertTrue(PipeStopStrategy.accept(new IOException("network disconnected"), null));
    Assert.assertTrue(
        PipeStopStrategy.accept(
            null, new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())));
  }

  @Test
  public void testRecordedMarkerIsFoundWhenResourceFailureIsWrapped() {
    final Exception failure =
        new IOException(
            new PipeRuntimeSinkResourceException(
                "retry queue exhausted", PipeResourceFailureType.NETWORK_TIMEOUT, true));

    Assert.assertTrue(PipeStopStrategy.isResourceFailureRecorded(failure));
  }
}
