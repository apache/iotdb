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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.db.utils.TestFileEncryptionParameterProvider;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class TsFileInsertionEventScanParserContextTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create("root.context.d1");
  private static final MeasurementSchema SCHEMA =
      new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);

  @Before
  public void setUp() {
    TestFileEncryptionParameterProvider.enable();
  }

  @After
  public void tearDown() {
    try {
      assertEquals(
          TestFileEncryptionParameterProvider.CREATED.get(),
          TestFileEncryptionParameterProvider.CLOSED.get());
    } finally {
      TestFileEncryptionParameterProvider.disable();
    }
  }

  @Test
  public void testSinglePageConstructorFailureCanRetry() throws Exception {
    assertConstructorFailureCanRetry(false);
  }

  @Test
  public void testMultiPageConstructorFailureCanRetry() throws Exception {
    assertConstructorFailureCanRetry(true);
  }

  private void assertConstructorFailureCanRetry(boolean multiPage) throws Exception {
    TsFileInsertionEventScanParser parser =
        mock(TsFileInsertionEventScanParser.class, CALLS_REAL_METHODS);
    List<Chunk> chunks = readChunks(multiPage);
    try {
      Chunk time = chunks.get(0);
      Chunk originalValue = chunks.get(1);
      ChunkHeader valueHeader = spy(originalValue.getHeader());
      Chunk value =
          new Chunk(
              valueHeader,
              originalValue.getData().duplicate(),
              originalValue.getDeleteIntervalList(),
              originalValue.getChunkStatistic(),
              originalValue.getEncryptParam());
      chunks.set(1, value);
      originalValue.close();
      AtomicBoolean fail = new AtomicBoolean(true);
      PipeRuntimeOutOfMemoryCriticalException expected =
          new PipeRuntimeOutOfMemoryCriticalException("test constructor memory pressure");
      Answer<Object> failOnce =
          call -> {
            if (fail.getAndSet(false)) {
              throw expected;
            }
            return call.callRealMethod();
          };
      if (multiPage) {
        doAnswer(failOnce).when(valueHeader).calculateDecoderForNonTimeChunk();
      } else {
        doAnswer(failOnce).when(valueHeader).getEncodingType();
      }
      assertSame(
          expected,
          assertThrows(
              PipeRuntimeOutOfMemoryCriticalException.class,
              () -> {
                try (IChunkReader ignored =
                    parser.createAlignedChunkReader(
                        time,
                        Collections.singletonList(value),
                        multiPage,
                        Collections.emptyList())) {
                  // Unexpected success must not leak a reader while reporting the assertion.
                }
              }));
      assertFalse(time.getEncryptParam().isDestroyed());
      assertFalse(value.getEncryptParam().isDestroyed());
      assertEquals(0, time.getData().position());
      assertEquals(0, value.getData().position());

      try (IChunkReader reader =
          parser.createAlignedChunkReader(
              time, Collections.singletonList(value), multiPage, Collections.emptyList())) {
        // Once construction succeeds, the pending group can release its original value chunks.
        value.close();
        int row = 0;
        while (reader.hasNextSatisfiedPage()) {
          BatchData data = reader.nextPageData();
          while (data.hasCurrent()) {
            assertEquals(row, data.currentTime());
            assertEquals(row * 10L, data.getVector()[0].getLong());
            row++;
            data.next();
          }
        }
        assertEquals(20, row);
      }
      assertFalse(time.getEncryptParam().isDestroyed());
    } finally {
      chunks.forEach(Chunk::close);
    }
  }

  @Test
  public void testPartialRetainFailureReleasesAttemptOnly() throws Exception {
    TsFileInsertionEventScanParser parser =
        mock(TsFileInsertionEventScanParser.class, CALLS_REAL_METHODS);
    List<Chunk> chunks = readChunks(false);
    try {
      Chunk broken = spy(chunks.get(1));
      IllegalStateException expected = new IllegalStateException("test retaining failure");
      doThrow(expected).when(broken).getData();
      assertSame(
          expected,
          assertThrows(
              IllegalStateException.class,
              () ->
                  parser.createAlignedChunkReader(
                      chunks.get(0),
                      Arrays.asList(chunks.get(1), broken),
                      false,
                      Collections.emptyList())));
      for (Chunk chunk : chunks) {
        assertFalse(chunk.getEncryptParam().isDestroyed());
      }
    } finally {
      chunks.forEach(Chunk::close);
    }
  }

  private List<Chunk> readChunks(boolean multiPage) throws Exception {
    File file = new File(folder.getRoot(), "aligned.tsfile");
    try (EncryptParameter parameter = TestFileEncryptionParameterProvider.newParameter();
        TsFileIOWriter writer = new TsFileIOWriter(file, parameter)) {
      writer.startChunkGroup(DEVICE);
      AlignedChunkWriterImpl chunk =
          new AlignedChunkWriterImpl(
              Collections.singletonList(SCHEMA), writer.getEncryptParameter());
      for (int i = 0; i < 20; i++) {
        chunk.getTimeChunkWriter().write(i);
        chunk.getValueChunkWriterByIndex(0).write(i, i * 10L, false);
        if (multiPage && i == 9) {
          chunk.sealCurrentPage();
        }
      }
      chunk.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      AbstractAlignedChunkMetadata metadata = reader.getAlignedChunkMetadata(DEVICE, false).get(0);
      return Arrays.asList(
          reader.readMemChunk((ChunkMetadata) metadata.getTimeChunkMetadata()),
          reader.readMemChunk((ChunkMetadata) metadata.getValueChunkMetadataList().get(0)));
    }
  }
}
