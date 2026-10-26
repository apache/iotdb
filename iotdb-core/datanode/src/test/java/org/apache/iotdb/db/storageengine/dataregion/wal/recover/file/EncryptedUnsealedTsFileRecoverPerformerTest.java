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

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.enums.TSDataType;
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
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
