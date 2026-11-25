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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkRetryTimesConfigurableException;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.rpc.AutoResizingBufferMemoryAllocationException;
import org.apache.iotdb.rpc.TElasticFramedTransport;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketException;

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
  public void testAutoResizingBufferAllocationFailureDoesNotStopPipe() {
    final Exception failure =
        new TTransportException(
            new AutoResizingBufferMemoryAllocationException("buffer allocation failed"));

    Assert.assertFalse(PipeStopStrategy.accept(failure, null));
    Assert.assertEquals(
        PipeResourceFailureType.MEMORY_TIMEOUT,
        PipeStopStrategy.getResourceFailureType(failure, null));
  }

  @Test
  public void testBrokenPipeFromReceiverResourcePressureDoesNotStopPipe() {
    final Exception failure =
        retryTimesConfigurableFailure(new TTransportException(new IOException("Broken pipe")));

    Assert.assertFalse(PipeStopStrategy.accept(failure, null));
    Assert.assertEquals(
        PipeResourceFailureType.NETWORK_TIMEOUT,
        PipeStopStrategy.getResourceFailureType(failure, null));
  }

  @Test
  public void testPeerClosedTransportDoesNotStopPipe() {
    final Exception failure =
        retryTimesConfigurableFailure(
            new TTransportException(
                TTransportException.END_OF_FILE, new IOException("Connection reset")));

    Assert.assertFalse(PipeStopStrategy.accept(failure, null));
    Assert.assertEquals(
        PipeResourceFailureType.NETWORK_TIMEOUT,
        PipeStopStrategy.getResourceFailureType(failure, null));
  }

  @Test
  public void testElasticFramedTransportResetRemainsRetryable() throws Exception {
    try (TElasticFramedTransport transport =
        new TElasticFramedTransport(new ConnectionResetTransport(), 100, 1024, true)) {
      final TTransportException failure =
          Assert.assertThrows(TTransportException.class, () -> transport.read(new byte[1], 0, 1));

      Assert.assertEquals(TTransportException.END_OF_FILE, failure.getType());
      Assert.assertFalse(PipeStopStrategy.accept(failure, null));
      Assert.assertEquals(
          PipeResourceFailureType.NETWORK_TIMEOUT,
          PipeStopStrategy.getResourceFailureType(failure, null));
    }
  }

  @Test
  public void testConnectionResetVariantsRemainRetryableThroughCriticalWrapper() {
    final Exception unixReset =
        retryTimesConfigurableFailure(
            new TTransportException(new SocketException("Connection reset")));
    Assert.assertFalse(PipeStopStrategy.accept(unixReset, null));
    Assert.assertEquals(
        PipeResourceFailureType.NETWORK_TIMEOUT,
        PipeStopStrategy.getResourceFailureType(unixReset, null));

    final Exception windowsReset =
        retryTimesConfigurableFailure(
            new TTransportException(
                new SocketException(
                    "An existing connection was forcibly closed by the remote host")));
    Assert.assertFalse(PipeStopStrategy.accept(windowsReset, null));
    Assert.assertEquals(
        PipeResourceFailureType.NETWORK_TIMEOUT,
        PipeStopStrategy.getResourceFailureType(windowsReset, null));
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

  private static class ConnectionResetTransport extends TTransport {

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public int read(final byte[] buffer, final int offset, final int length)
        throws TTransportException {
      throw new TTransportException(new SocketException("Connection reset"));
    }

    @Override
    public void write(final byte[] buffer, final int offset, final int length) {}

    @Override
    public TConfiguration getConfiguration() {
      return new TConfiguration();
    }

    @Override
    public void updateKnownMessageSize(final long size) {}

    @Override
    public void checkReadBytesAvailable(final long numBytes) {}
  }

  private static PipeRuntimeSinkRetryTimesConfigurableException retryTimesConfigurableFailure(
      final Throwable cause) {
    final PipeRuntimeSinkRetryTimesConfigurableException exception =
        new PipeRuntimeSinkRetryTimesConfigurableException("transfer failed", Integer.MAX_VALUE);
    exception.initCause(cause);
    return exception;
  }
}
