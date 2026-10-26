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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskAlignedChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FileEncryptionContextTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create("root.context.d1");
  private static final MeasurementSchema SCHEMA =
      new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);

  @After
  public void tearDown() throws Exception {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    ChunkCache.getInstance().clear();
    TestFileEncryptionParameterProvider.disable();
  }

  @Test
  public void testDefaultWriterPreservesPlainFileFormat() throws Exception {
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      write(writer);
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      assertFalse(reader.hasFileEncryptionHeader());
      assertEquals(TSFileConfig.MAGIC_STRING.length() + 1, reader.getDataStartOffset());
    }
    assertFalse(EncryptDBUtils.requiresFileRewrite());
  }

  @Test
  public void testEachWriterGetsIndependentFileContext() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    byte[][] ids = new byte[2][];
    for (int i = 0; i < 2; i++) {
      File file = file((i + 1) + "-1-0-0.tsfile");
      try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
        ids[i] = writer.getEncryptParameter().getFileCryptoId();
        write(writer);
      }
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
        assertTrue(reader.hasFileEncryptionHeader());
        assertTrue(reader.getDataStartOffset() > TSFileConfig.MAGIC_STRING.length() + 1);
        assertArrayEquals(ids[i], reader.getEncryptParam().getFileCryptoId());
      }
    }
    assertFalse(Arrays.equals(ids[0], ids[1]));
    assertTrue(EncryptDBUtils.requiresFileRewrite());
    assertEquals(
        TestFileEncryptionParameterProvider.CREATED.get(),
        TestFileEncryptionParameterProvider.CLOSED.get());
  }

  @Test
  public void testLazyChunkOutlivesFileReaderAndClosesAfterLastPage() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      write(writer);
    }
    QueryContext context = externalContext();
    ChunkMetadata metadata = metadata(file);
    IChunkReader reader =
        new DiskChunkLoader(context, new TsFileResource(file)).getChunkReader(metadata, null);
    List<IPageReader> pages = reader.loadPageReaderList();
    assertEquals(2, pages.size());
    assertEquals(2, trackedPages(context));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    int row = 0;
    for (IPageReader page : pages) {
      BatchData data = page.getAllSatisfiedPageData();
      while (data.hasCurrent()) {
        assertEquals(row, data.currentTime());
        assertEquals(row * 10L, data.getLong());
        row++;
        data.next();
      }
      context.getQueryChunkReaders().release(page);
    }
    assertEquals(20, row);
    assertEquals(0, trackedPages(context));
    context.getQueryChunkReaders().close();
    assertEquals(
        TestFileEncryptionParameterProvider.CREATED.get(),
        TestFileEncryptionParameterProvider.CLOSED.get());
  }

  @Test
  public void testEarlyQueryCloseReleasesUnconsumedContext() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      write(writer);
    }
    QueryContext context = externalContext();
    new DiskChunkLoader(context, new TsFileResource(file)).getChunkReader(metadata(file), null);
    assertEquals(2, trackedPages(context));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    context.getQueryChunkReaders().close();
    assertEquals(
        TestFileEncryptionParameterProvider.CREATED.get(),
        TestFileEncryptionParameterProvider.CLOSED.get());
  }

  @Test
  public void testPlainChunksAreNotRetainedByQuery() throws Exception {
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      write(writer);
    }
    QueryContext context = externalContext();
    try (IChunkReader reader =
        new DiskChunkLoader(context, new TsFileResource(file))
            .getChunkReader(metadata(file), null)) {
      assertEquals(2, reader.loadPageReaderList().size());
      assertEquals(0, trackedPages(context));
    }
  }

  @Test
  public void testMemTableKeepsExplicitContextAcrossDatabaseAssignmentAndCopy() {
    try (EncryptParameter parameter = TestFileEncryptionParameterProvider.newParameter()) {
      PrimitiveMemTable table = new PrimitiveMemTable(null, null);
      table.setEncryptParameter(EncryptUtils.getEncryptParameter(parameter));
      table.setDatabaseAndDataRegionId("root.context", "0");
      assertTrue(table.getEncryptParameter().sharesPageAeadFileContext(parameter));
      assertSame(table.getEncryptParameter(), table.copy().getEncryptParameter());
    }
  }

  private static QueryContext externalContext() {

    return new QueryContext(false, false) {
      @Override
      public boolean isExternalTsFileScan() {
        return true;
      }
    };
  }

  @Test
  public void testNullProviderCannotSilentlyCreatePlainFile() throws Exception {
    TestFileEncryptionParameterProvider.RETURN_NULL.set(true);
    File file = file("1-1-0-0.tsfile");
    assertThrows(
        IllegalStateException.class,
        () -> EncryptDBUtils.createWriterForNewFile(file, "root.context"));
    assertFalse(file.exists());
  }

  @Test
  public void testCacheEvictionDoesNotInvalidateRetainedLazyPages() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      write(writer);
    }
    QueryContext context = new QueryContext(false, false);
    IChunkReader chunkReader =
        new DiskChunkLoader(context, new TsFileResource(file)).getChunkReader(metadata(file), null);
    ChunkCache.getInstance().clear();
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    int rows = 0;
    for (IPageReader page : chunkReader.loadPageReaderList()) {
      BatchData data = page.getAllSatisfiedPageData();
      rows += data.length();
      context.getQueryChunkReaders().release(page);
    }
    assertEquals(20, rows);
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertEquals(
                    TestFileEncryptionParameterProvider.CREATED.get(),
                    TestFileEncryptionParameterProvider.CLOSED.get()));
  }

  @Test
  public void testAlignedReaderOwnsTimeAndValueContextsUntilQueryClose() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      writer.startChunkGroup(DEVICE);
      AlignedChunkWriterImpl chunk =
          new AlignedChunkWriterImpl(
              Collections.singletonList(SCHEMA),
              EncryptUtils.getEncryptParameter(writer.getEncryptParameter()));
      for (int i = 0; i < 20; i++) {
        chunk.getTimeChunkWriter().write(i);
        chunk.getValueChunkWriterByIndex(0).write(i, i * 10L, false);
        if (i == 9) {
          chunk.sealCurrentPage();
        }
      }
      chunk.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
    AbstractAlignedChunkMetadata metadata;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      metadata = reader.getAlignedChunkMetadata(DEVICE, false).get(0);
    }
    QueryContext context = externalContext();
    IChunkReader chunkReader =
        new DiskAlignedChunkLoader(context, new TsFileResource(file), false)
            .getChunkReader(metadata, null);
    assertEquals(2, trackedPages(context));
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    int rows = 0;
    for (IPageReader page : chunkReader.loadPageReaderList()) {
      rows += page.getAllSatisfiedPageData().length();
    }
    assertEquals(20, rows);
    context.getQueryChunkReaders().close();
    assertEquals(
        TestFileEncryptionParameterProvider.CREATED.get(),
        TestFileEncryptionParameterProvider.CLOSED.get());
  }

  @Test
  public void testCompactionWriterUsesAndReleasesItsOwnFileContext() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    File file = file("1-1-0-0.tsfile");
    try (CompactionTsFileWriter writer =
        CompactionTsFileWriter.create(
            file,
            1024 * 1024,
            CompactionType.INNER_SEQ_COMPACTION,
            EncryptDBUtils.getDefaultFirstEncryptParam())) {
      assertTrue(writer.getEncryptParameter().isTdePageAead());
      write(writer);
    }
    assertEquals(
        TestFileEncryptionParameterProvider.CREATED.get(),
        TestFileEncryptionParameterProvider.CLOSED.get());
  }

  @Test
  public void testMissingProviderFailsClosedWhenOpeningFile() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      write(writer);
    }
    TestFileEncryptionParameterProvider.disable();
    assertThrows(
        Exception.class,
        () -> {
          try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
            reader.readMemChunk(metadata(file));
          }
        });
  }

  @Test
  public void testEncryptedSplitterAuthenticatesAndRewritesTransportChunks() throws Exception {
    TestFileEncryptionParameterProvider.enable();
    File file = file("1-1-0-0.tsfile");
    try (TsFileIOWriter writer = EncryptDBUtils.createWriterForNewFile(file, "root.context")) {
      write(writer);
    }
    List<Chunk> chunks = new ArrayList<>();
    new TsFileSplitter(
            file,
            data -> {
              if (data instanceof ChunkData) {
                chunks.addAll(((ChunkData) data).getChunks());
              }
              return true;
            })
        .splitTsFileByDataPartition();
    assertFalse(chunks.isEmpty());
    int row = 0;
    for (Chunk chunk : chunks) {
      assertTrue(chunk.getHeader().getChunkOrdinal() < 0);
      try (ChunkReader reader = new ChunkReader(chunk)) {
        while (reader.hasNextSatisfiedPage()) {
          BatchData data = reader.nextPageData();
          while (data.hasCurrent()) {
            assertEquals(row, data.currentTime());
            assertEquals(row * 10L, data.getLong());
            row++;
            data.next();
          }
        }
      }
    }
    assertEquals(20, row);
  }

  private static int trackedPages(QueryContext context) throws Exception {
    Field field = context.getQueryChunkReaders().getClass().getDeclaredField("pages");
    field.setAccessible(true);
    return ((Map<?, ?>) field.get(context.getQueryChunkReaders())).size();
  }

  private static ChunkMetadata metadata(File file) throws Exception {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      return (ChunkMetadata)
          reader.getAllTimeseriesMetadata(true).get(DEVICE).get(0).getChunkMetadataList().get(0);
    }
  }

  private File file(String name) throws Exception {
    File file = new File(folder.getRoot(), "sequence/root.context/0/0/" + name);
    Files.createDirectories(file.toPath().getParent());
    return file;
  }

  private static void write(TsFileIOWriter writer) throws Exception {
    writer.startChunkGroup(DEVICE);
    ChunkWriterImpl chunk =
        new ChunkWriterImpl(SCHEMA, EncryptUtils.getEncryptParameter(writer.getEncryptParameter()));
    for (int i = 0; i < 20; i++) {
      chunk.write(i, i * 10L);
      if (i == 9) {
        chunk.sealCurrentPage();
      }
    }
    chunk.writeToFileWriter(writer);
    writer.endChunkGroup();
    writer.endFile();
  }
}
