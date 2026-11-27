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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferIntegrityUtils;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeAsyncClientManager;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeTransferTsFileHandlerCleanupTest {

  @Test
  public void testCloseDeletesBatchFile() throws Exception {
    final File file = Files.createTempFile("pipe-transfer-batch", ".tsfile").toFile();
    final EnrichedEvent event = Mockito.mock(EnrichedEvent.class);

    createHandler(file, event).close();

    Assert.assertFalse(file.exists());
  }

  @Test
  public void testCloseDeletesBatchFileWhenReaderCloseFails() throws Exception {
    final File file = Files.createTempFile("pipe-transfer-close-failure", ".tsfile").toFile();
    final EnrichedEvent event = Mockito.mock(EnrichedEvent.class);
    final PipeTransferTsFileHandler handler = createHandler(file, event);
    final RandomAccessFile reader = Mockito.mock(RandomAccessFile.class);
    Mockito.doThrow(new IOException("close failed")).when(reader).close();
    final Field readerField = PipeTransferTsFileHandler.class.getDeclaredField("reader");
    readerField.setAccessible(true);
    readerField.set(handler, reader);

    handler.close();

    Mockito.verify(reader, Mockito.atLeastOnce()).close();
    Assert.assertFalse(file.exists());
  }

  @Test
  public void testNullClientDeletesBatchFile() throws Exception {
    final File file = Files.createTempFile("pipe-transfer-null-client", ".tsfile").toFile();
    final EnrichedEvent event = Mockito.mock(EnrichedEvent.class);
    final PipeTransferTsFileHandler handler = createHandler(file, event);

    handler.transfer(null, null);

    Assert.assertFalse(file.exists());
  }

  @Test
  public void testCloseKeepsSourceTsFile() throws Exception {
    final File file = Files.createTempFile("pipe-transfer-source", ".tsfile").toFile();
    final PipeTsFileInsertionEvent event = Mockito.mock(PipeTsFileInsertionEvent.class);
    try {
      createHandler(file, event).close();
      Assert.assertTrue(file.exists());
    } finally {
      if (file.exists()) {
        Assert.assertTrue(file.delete());
      }
    }
  }

  @Test
  public void testModFileDisappearingBeforeOpenDowngradesToTsFileOnly() throws Exception {
    final File tsFile = Files.createTempFile("pipe-transfer-source", ".tsfile").toFile();
    final File modFile =
        new DisappearingFile(
            Files.createTempDirectory("pipe-transfer-missing-mod")
                .resolve("source.tsfile.mods2")
                .toString());
    final PipeTsFileInsertionEvent event = Mockito.mock(PipeTsFileInsertionEvent.class);
    final PipeTransferTsFileHandler handler =
        new PipeTransferTsFileHandler(
            Mockito.mock(IoTDBDataRegionAsyncSink.class),
            Collections.emptyMap(),
            Collections.singletonList(event),
            new AtomicInteger(1),
            new AtomicBoolean(false),
            tsFile,
            modFile,
            true,
            null);

    try {
      final Method openNextFile = PipeTransferTsFileHandler.class.getDeclaredMethod("openNextFile");
      openNextFile.setAccessible(true);
      Assert.assertTrue((Boolean) openNextFile.invoke(handler));

      final Field currentFile = PipeTransferTsFileHandler.class.getDeclaredField("currentFile");
      currentFile.setAccessible(true);
      Assert.assertSame(tsFile, currentFile.get(handler));

      final Field effectiveTransferMod =
          PipeTransferTsFileHandler.class.getDeclaredField("effectiveTransferMod");
      effectiveTransferMod.setAccessible(true);
      Assert.assertFalse(effectiveTransferMod.getBoolean(handler));
    } finally {
      handler.close();
      Assert.assertTrue(tsFile.delete());
      Assert.assertTrue(modFile.getParentFile().delete());
    }
  }

  @Test
  public void testSealFailurePassesNestedReceiverMessageToRetryQueue() throws Exception {
    final File file = Files.createTempFile("pipe-transfer-seal-failure", ".tsfile").toFile();
    try {
      final PipeTsFileInsertionEvent event = Mockito.mock(PipeTsFileInsertionEvent.class);
      final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
      Mockito.when(sink.statusHandler())
          .thenReturn(new PipeReceiverStatusHandler(false, 60, false, 60, false, false));

      final PipeTransferTsFileHandler handler =
          new PipeTransferTsFileHandler(
              sink,
              Collections.emptyMap(),
              Collections.singletonList(event),
              new AtomicInteger(1),
              new AtomicBoolean(false),
              file,
              null,
              false,
              null);
      markSealSignalSent(handler);

      final TSStatus status =
          new TSStatus(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
              .setMessage("aggregate load failure")
              .setSubStatus(
                  Collections.singletonList(
                      new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
                          .setMessage("receiver disk is full")));

      Assert.assertFalse(handler.onCompleteInternal(new TPipeTransferResp(status)));

      final ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
      Mockito.verify(sink)
          .addFailureEventsToRetryQueue(
              Mockito.eq(Collections.singletonList(event)),
              exceptionCaptor.capture(),
              Mockito.eq(handler));
      Assert.assertEquals("receiver disk is full", exceptionCaptor.getValue().getMessage());
    } finally {
      if (file.exists()) {
        Assert.assertTrue(file.delete());
      }
    }
  }

  @Test
  public void testStructuralCorruptionRetriesTsFileWithCrc() throws Exception {
    final File file = Files.createTempFile(null, null).toFile();
    final RecordingRetryHandler handler =
        new RecordingRetryHandler(file, Mockito.mock(PipeTsFileInsertionEvent.class));
    try {
      markSealSignalSent(handler);
      final TSStatus status =
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
              .setMessage(PipeTransferIntegrityUtils.STRUCTURE_CORRUPTION_MARKER);
      Assert.assertFalse(handler.onCompleteInternal(new TPipeTransferResp(status)));
      Assert.assertEquals(1, handler.transferCount);
      Assert.assertTrue(handler.isCrcEnabled());
    } finally {
      handler.close();
      Files.deleteIfExists(file.toPath());
    }
  }

  @Test
  public void testStructuralCorruptionRetryClosesReaderAndRestartsAtBeginning() throws Exception {
    final File file = Files.createTempFile("pipe-transfer-structural-retry", ".tsfile").toFile();
    Files.write(file.toPath(), new byte[] {1, 2, 3});
    final RecordingRetryHandler handler =
        new RecordingRetryHandler(file, Mockito.mock(PipeTsFileInsertionEvent.class));
    final RandomAccessFile staleReader = new RandomAccessFile(file, "r");
    staleReader.seek(file.length());
    try {
      setHandlerField(handler, "reader", staleReader);
      setHandlerField(handler, "transferringTsFile", true);
      setHandlerField(handler, "position", file.length());
      markSealSignalSent(handler);
      final TSStatus status =
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
              .setMessage(PipeTransferIntegrityUtils.STRUCTURE_CORRUPTION_MARKER);

      Assert.assertFalse(handler.onCompleteInternal(new TPipeTransferResp(status)));

      Assert.assertEquals(1, handler.transferCount);
      Assert.assertNull(getHandlerField(handler, "reader"));
      Assert.assertEquals(0L, getHandlerField(handler, "position"));
      Assert.assertFalse((boolean) getHandlerField(handler, "transferringTsFile"));
      Assert.assertSame(file, getHandlerField(handler, "currentFile"));
      Assert.assertThrows(IOException.class, staleReader::getFilePointer);

      final Method openNextFile = PipeTransferTsFileHandler.class.getDeclaredMethod("openNextFile");
      openNextFile.setAccessible(true);
      Assert.assertTrue((Boolean) openNextFile.invoke(handler));
      Assert.assertEquals(
          0L, ((RandomAccessFile) getHandlerField(handler, "reader")).getFilePointer());
    } finally {
      handler.close();
      Files.deleteIfExists(file.toPath());
    }
  }

  private static void markSealSignalSent(final PipeTransferTsFileHandler handler) throws Exception {
    final Field field = PipeTransferTsFileHandler.class.getDeclaredField("isSealSignalSent");
    field.setAccessible(true);
    ((AtomicBoolean) field.get(handler)).set(true);
  }

  private static Object getHandlerField(final PipeTransferTsFileHandler handler, final String name)
      throws Exception {
    final Field field = PipeTransferTsFileHandler.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(handler);
  }

  private static void setHandlerField(
      final PipeTransferTsFileHandler handler, final String name, final Object value)
      throws Exception {
    final Field field = PipeTransferTsFileHandler.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(handler, value);
  }

  private PipeTransferTsFileHandler createHandler(final File file, final EnrichedEvent event)
      throws Exception {
    return new PipeTransferTsFileHandler(
        Mockito.mock(IoTDBDataRegionAsyncSink.class),
        Collections.emptyMap(),
        Collections.singletonList(event),
        new AtomicInteger(1),
        new AtomicBoolean(false),
        file,
        null,
        false,
        null);
  }

  private static final class RecordingRetryHandler extends PipeTransferTsFileHandler {

    private int transferCount;

    private RecordingRetryHandler(final File file, final PipeTsFileInsertionEvent event)
        throws InterruptedException {
      super(
          Mockito.mock(IoTDBDataRegionAsyncSink.class),
          Collections.emptyMap(),
          Collections.singletonList(event),
          new AtomicInteger(1),
          new AtomicBoolean(false),
          file,
          null,
          false,
          null);
    }

    private boolean isCrcEnabled() throws Exception {
      final Field field = PipeTransferTsFileHandler.class.getDeclaredField("crcEnabled");
      field.setAccessible(true);
      return field.getBoolean(this);
    }

    @Override
    public void transfer(
        final IoTDBDataNodeAsyncClientManager clientManager,
        final AsyncPipeDataTransferServiceClient client) {
      transferCount++;
    }
  }

  private static final class DisappearingFile extends File {

    private final AtomicInteger existenceCheckCount = new AtomicInteger();

    private DisappearingFile(final String pathname) {
      super(pathname);
    }

    @Override
    public boolean exists() {
      return existenceCheckCount.incrementAndGet() <= 2;
    }
  }
}
