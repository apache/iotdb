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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover.file;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.TsFileUtilsForRecoverTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.TestFileEncryptionParameterProvider;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class EncryptedUnsealedTsFileRecoverPerformerTest {
  private static final String DATABASE = "root.context_recovery";
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(DATABASE + ".d1");
  private static final MeasurementSchema SCHEMA =
      new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
  private static final String FILE_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(DATABASE, 0, 0, 1);
  private TsFileResource resource;
  private int nextFileId = 100;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(new File(FILE_NAME).getParent());
    EnvironmentUtils.envSetUp();
    TestFileEncryptionParameterProvider.enable();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (resource != null) {
        resource.close();
      }
      EnvironmentUtils.cleanDir(new File(FILE_NAME).getParent());
      EnvironmentUtils.cleanEnv();
    } finally {
      TestFileEncryptionParameterProvider.disable();
    }
  }

  @Test
  public void testNonAlignedInsertRecovery() throws Exception {
    assertRecovery(false, false);
  }

  @Test
  public void testAlignedInsertRecovery() throws Exception {
    assertRecovery(true, false);
  }

  @Test
  public void testNonAlignedSnapshotRecovery() throws Exception {
    assertRecovery(false, true);
  }

  @Test
  public void testAlignedSnapshotRecovery() throws Exception {
    assertRecovery(true, true);
  }

  @Test
  public void testEmptyFileInsertRecoveryUsesProvider() throws Exception {
    assertNoDataRecovery(false, false, false);
    assertNoDataRecovery(false, true, false);
  }

  @Test
  public void testBaseHeaderInsertRecoveryUsesProvider() throws Exception {
    assertNoDataRecovery(true, false, false);
    assertNoDataRecovery(true, true, false);
  }

  @Test
  public void testEmptyFileSnapshotRecoveryUsesProvider() throws Exception {
    assertNoDataRecovery(false, false, true);
    assertNoDataRecovery(false, true, true);
  }

  @Test
  public void testBaseHeaderSnapshotRecoveryUsesProvider() throws Exception {
    assertNoDataRecovery(true, false, true);
    assertNoDataRecovery(true, true, true);
  }

  private void assertNoDataRecovery(boolean baseHeaderOnly, boolean aligned, boolean snapshot)
      throws Exception {
    File file = newRecoveryFile();
    byte[] oldFileId;
    try (EncryptParameter parameter = newFileParameter();
        TsFileIOWriter writer = new TsFileIOWriter(file, parameter)) {
      oldFileId = writer.getEncryptParameter().getFileCryptoId();
    }
    try (FileChannel channel = new FileOutputStream(file, true).getChannel()) {
      channel.truncate(baseHeaderOnly ? baseHeader().length : 0);
    }
    resource = new TsFileResource(file);
    byte[] recoveredFileId;
    try (UnsealedTsFileRecoverPerformer performer =
        new UnsealedTsFileRecoverPerformer(resource, true, ignored -> {})) {
      performer.startRecovery();
      assertTrue(performer.hasCrashed());
      EncryptParameter parameter = performer.getWriter().getEncryptParameter();
      assertTrue(parameter.isTdePageAead());
      recoveredFileId = parameter.getFileCryptoId();
      assertFalse(Arrays.equals(oldFileId, recoveredFileId));
      performer.redoLog(firstEntry(aligned, snapshot));
      performer.endRecovery();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      assertTrue(reader.hasFileEncryptionHeader());
      assertArrayEquals(recoveredFileId, reader.getEncryptParam().getFileCryptoId());
      assertSingleRow(reader, aligned);
    }
    resource.close();
  }

  @Test
  public void testProviderFailureDoesNotModifyUninitializedFiles() throws Exception {
    TestFileEncryptionParameterProvider.RETURN_NULL.set(true);
    for (boolean baseHeaderOnly : new boolean[] {false, true}) {
      File file = newRecoveryFile();
      byte[] original = baseHeaderOnly ? baseHeader() : new byte[0];
      Files.write(file.toPath(), original);
      resource = new TsFileResource(file);
      try (UnsealedTsFileRecoverPerformer performer =
          new UnsealedTsFileRecoverPerformer(resource, true, ignored -> {})) {
        assertThrows(RuntimeException.class, performer::startRecovery);
        assertNull(performer.getWriter());
      }
      assertArrayEquals(original, Files.readAllBytes(file.toPath()));
      assertFalse(resource.resourceFileExists());
      resource.close();
    }
  }

  @Test
  public void testPartialHeadersCannotBeReinitialized() throws Exception {
    byte[] base = baseHeader();
    byte[] partialEncryptionHeader = Arrays.copyOf(base, base.length + 1);
    partialEncryptionHeader[base.length] = MetaMarker.ENCRYPTION_HEADER;
    for (byte[] original :
        new byte[][] {Arrays.copyOf(base, base.length - 1), partialEncryptionHeader}) {
      File file = newRecoveryFile();
      Files.write(file.toPath(), original);
      resource = new TsFileResource(file);
      try (UnsealedTsFileRecoverPerformer performer =
          new UnsealedTsFileRecoverPerformer(resource, true, ignored -> {})) {
        assertThrows(Exception.class, performer::startRecovery);
      }
      assertArrayEquals(original, Files.readAllBytes(file.toPath()));
      resource.close();
    }
  }

  @Test
  public void testValidEncryptionHeaderWithoutDataKeepsOriginalContext() throws Exception {
    File file = newRecoveryFile();
    byte[] oldFileId;
    long headerEnd;
    try (EncryptParameter parameter = newFileParameter();
        TsFileIOWriter writer = new TsFileIOWriter(file, parameter)) {
      oldFileId = writer.getEncryptParameter().getFileCryptoId();
      headerEnd = writer.getPos();
      writer.endFile();
    }
    try (FileChannel channel = new FileOutputStream(file, true).getChannel()) {
      channel.truncate(headerEnd);
    }
    resource = new TsFileResource(file);
    try (UnsealedTsFileRecoverPerformer performer =
        new UnsealedTsFileRecoverPerformer(resource, true, ignored -> {})) {
      performer.startRecovery();
      assertArrayEquals(oldFileId, performer.getWriter().getEncryptParameter().getFileCryptoId());
      performer.redoLog(firstEntry(false, false));
      performer.endRecovery();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      assertArrayEquals(oldFileId, reader.getEncryptParam().getFileCryptoId());
      assertSingleRow(reader, false);
    }
  }

  private File newRecoveryFile() throws Exception {
    File file = new File(new File(FILE_NAME).getParentFile(), nextFileId++ + "-1-0-0.tsfile");
    Files.createDirectories(file.getParentFile().toPath());
    Files.createFile(file.toPath());
    return file;
  }

  private static byte[] baseHeader() {
    byte[] magic = TSFileConfig.MAGIC_STRING.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(magic.length + Byte.BYTES)
        .put(magic)
        .put(TSFileConfig.VERSION_NUMBER)
        .array();
  }

  private static EncryptParameter newFileParameter() {
    return TestFileEncryptionParameterProvider.newParameter();
  }

  private static WALEntry firstEntry(boolean aligned, boolean snapshot) throws Exception {
    InsertRowNode row =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(DEVICE),
            aligned,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            1,
            new Object[] {10L},
            false);
    row.setMeasurementSchemas(new MeasurementSchema[] {SCHEMA});
    if (!snapshot) {
      return roundTrip(new WALInfoEntry(1, row));
    }
    PrimitiveMemTable table = new PrimitiveMemTable(DATABASE, "0");
    if (aligned) {
      table.insertAlignedRow(row);
    } else {
      table.insert(row);
    }
    return roundTrip(new WALInfoEntry(1, table));
  }

  private static void assertSingleRow(TsFileSequenceReader reader, boolean aligned)
      throws Exception {
    if (aligned) {
      AbstractAlignedChunkMetadata metadata = reader.getAlignedChunkMetadata(DEVICE, false).get(0);
      try (Chunk time = reader.readMemChunk((ChunkMetadata) metadata.getTimeChunkMetadata());
          Chunk value =
              reader.readMemChunk((ChunkMetadata) metadata.getValueChunkMetadataList().get(0));
          IChunkReader chunks =
              new AlignedChunkReader(time, Collections.singletonList(value), null)) {
        assertEquals(1, assertRows(chunks, 0, true));
      }
    } else {
      ChunkMetadata metadata = reader.getChunkMetadataList(new Path(DEVICE, "s1", true)).get(0);
      try (Chunk chunk = reader.readMemChunk(metadata);
          IChunkReader chunks = new ChunkReader(chunk)) {
        assertEquals(1, assertRows(chunks, 0, false));
      }
    }
  }

  private void assertRecovery(boolean aligned, boolean snapshot) throws Exception {
    File file = new File(FILE_NAME);
    Files.createDirectories(file.getParentFile().toPath());
    byte[] originalFileId;
    long dataEnd;
    try (EncryptParameter parameter = TestFileEncryptionParameterProvider.newParameter();
        TsFileIOWriter writer = new TsFileIOWriter(file, parameter)) {
      originalFileId = writer.getEncryptParameter().getFileCryptoId();
      writer.startChunkGroup(DEVICE);
      if (aligned) {
        AlignedChunkWriterImpl chunk =
            new AlignedChunkWriterImpl(
                Collections.singletonList(SCHEMA), writer.getEncryptParameter());
        chunk.getTimeChunkWriter().write(1);
        chunk.getValueChunkWriterByIndex(0).write(1, 10L, false);
        chunk.writeToFileWriter(writer);
      } else {
        ChunkWriterImpl chunk = new ChunkWriterImpl(SCHEMA, writer.getEncryptParameter());
        chunk.write(1, 10L);
        chunk.writeToFileWriter(writer);
      }
      writer.endChunkGroup();
      writer.writePlanIndices();
      dataEnd = writer.getPos();
      writer.endFile();
    }
    try (FileChannel channel = new FileOutputStream(file, true).getChannel()) {
      channel.truncate(dataEnd);
    }

    InsertRowNode row =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(DEVICE),
            aligned,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            2,
            new Object[] {20L},
            false);
    row.setMeasurementSchemas(new MeasurementSchema[] {SCHEMA});
    WALEntry entry;
    if (snapshot) {
      PrimitiveMemTable table = new PrimitiveMemTable(DATABASE, "0");
      if (aligned) {
        table.insertAlignedRow(row);
      } else {
        table.insert(row);
      }
      entry = roundTrip(new WALInfoEntry(1, table));
      assertFalse(((PrimitiveMemTable) entry.getValue()).getEncryptParameter().isTdePageAead());
    } else {
      entry = roundTrip(new WALInfoEntry(1, row));
    }

    resource = new TsFileResource(file);
    try (UnsealedTsFileRecoverPerformer performer =
        new UnsealedTsFileRecoverPerformer(resource, true, ignored -> {})) {
      performer.startRecovery();
      assertTrue(performer.hasCrashed());
      assertArrayEquals(
          originalFileId, performer.getWriter().getEncryptParameter().getFileCryptoId());
      performer.redoLog(entry);
      if (snapshot) {
        assertTrue(
            ((PrimitiveMemTable) entry.getValue())
                .getEncryptParameter()
                .sharesPageAeadFileContext(performer.getWriter().getEncryptParameter()));
      }
      performer.endRecovery();
    }

    Set<Long> ordinals = new HashSet<>();
    int rows = 0;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      assertArrayEquals(originalFileId, reader.getEncryptParam().getFileCryptoId());
      if (aligned) {
        for (AbstractAlignedChunkMetadata metadata :
            reader.getAlignedChunkMetadata(DEVICE, false)) {
          try (Chunk time = reader.readMemChunk((ChunkMetadata) metadata.getTimeChunkMetadata());
              Chunk value =
                  reader.readMemChunk((ChunkMetadata) metadata.getValueChunkMetadataList().get(0));
              IChunkReader chunkReader =
                  new AlignedChunkReader(time, Collections.singletonList(value), null)) {
            assertTrue(ordinals.add(time.getHeader().getChunkOrdinal()));
            assertTrue(ordinals.add(value.getHeader().getChunkOrdinal()));
            rows = assertRows(chunkReader, rows, true);
          }
        }
      } else {
        for (ChunkMetadata metadata : reader.getChunkMetadataList(new Path(DEVICE, "s1", true))) {
          try (Chunk chunk = reader.readMemChunk(metadata);
              IChunkReader chunkReader = new ChunkReader(chunk)) {
            assertTrue(ordinals.add(chunk.getHeader().getChunkOrdinal()));
            rows = assertRows(chunkReader, rows, false);
          }
        }
      }
    }
    assertEquals(2, rows);
    assertEquals(aligned ? 4 : 2, ordinals.size());
    assertEquals(2L, (long) resource.getEndTime(DEVICE).get());
  }

  private static WALEntry roundTrip(WALInfoEntry entry) throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(entry.serializedSize());
    entry.serialize(new WALByteBufferForTest(buffer));
    try (DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(buffer.array(), 0, buffer.position()))) {
      return WALEntry.deserialize(input);
    }
  }

  private static int assertRows(IChunkReader reader, int rows, boolean aligned) throws Exception {
    while (reader.hasNextSatisfiedPage()) {
      BatchData data = reader.nextPageData();
      while (data.hasCurrent()) {
        rows++;
        assertEquals(rows, data.currentTime());
        assertEquals(rows * 10L, aligned ? data.getVector()[0].getLong() : data.getLong());
        data.next();
      }
    }
    return rows;
  }
}
