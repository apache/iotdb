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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.reader;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkHeader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndexEntry;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndexHeader;
import org.apache.iotdb.lsm.sstable.fileIO.FileInput;
import org.apache.iotdb.lsm.sstable.fileIO.FileOutput;
import org.apache.iotdb.lsm.sstable.fileIO.IFileOutput;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChunkGroupReaderTest {
  File file;

  ChunkGroupReader chunkGroupReader;

  long chunkIndexOffset;

  @Before
  public void setUp() throws Exception {
    file = new File("ChunkGroupReaderTest");
    serializeChunkGroup();
    FileInput dataInput = new FileInput(file);
    chunkGroupReader = new ChunkGroupReader(dataInput, chunkIndexOffset);
  }

  @After
  public void tearDown() throws Exception {
    chunkGroupReader.close();
    file.delete();
  }

  @Test
  public void testReadChunkIndex() throws IOException {
    ChunkIndex chunkIndex = chunkGroupReader.readChunkIndex(chunkIndexOffset);
    assertEquals(chunkIndex.getChunkIndexHeader().getSize(), 2);
  }

  @Test
  public void testReadAllDeviceID() throws IOException {
    RoaringBitmap roaringBitmap = chunkGroupReader.readAllDeviceID(chunkIndexOffset);
    assertEquals(4101, roaringBitmap.getCardinality());
    for (int i = 0; i < 4097; i++) {
      assertTrue(roaringBitmap.contains(i * 6));
    }
    assertTrue(roaringBitmap.contains(100000));
    assertTrue(roaringBitmap.contains(50000000));
    assertTrue(roaringBitmap.contains(50000001));
    assertTrue(roaringBitmap.contains(1111111111));
  }

  @Test
  public void testIterator() throws IOException {
    int count = 0;
    while (chunkGroupReader.hasNext()) {
      int now = chunkGroupReader.next();
      if (count < 4097) {
        assertEquals(count * 6, now);
      } else if (count == 4097) {
        assertEquals(100000, now);
      } else if (count == 4098) {
        assertEquals(50000000, now);
      } else if (count == 4099) {
        assertEquals(50000001, now);
      } else {
        assertEquals(1111111111, now);
      }
      count++;
    }
  }

  private void serializeChunkGroup() throws IOException {
    FileOutput fileOutput = new FileOutput(file);
    List<ChunkIndexEntry> chunkIndexEntries = new ArrayList<>();
    chunkIndexEntries.add(serializeChunk1(fileOutput));
    chunkIndexEntries.add(serializeChunk2(fileOutput));

    ChunkIndexHeader chunkIndexHeader = new ChunkIndexHeader(2);
    ChunkIndex chunkIndex = new ChunkIndex(chunkIndexEntries, chunkIndexHeader);
    chunkIndexOffset = fileOutput.write(chunkIndex);
    fileOutput.close();
  }

  private ChunkIndexEntry serializeChunk1(IFileOutput output) throws IOException {
    RoaringBitmap a = new RoaringBitmap();
    for (int i = 0; i < 4097; i++) {
      a.add(i * 6);
    }
    a.add(100000);

    int size = a.serializedSizeInBytes();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    a.serialize(buffer);

    buffer.flip();
    output.write(buffer);
    ChunkHeader chunkHeader = new ChunkHeader(size);
    long offset = output.write(chunkHeader);
    return new ChunkIndexEntry(offset, 4098, 100000, 0);
  }

  private ChunkIndexEntry serializeChunk2(IFileOutput output) throws IOException {
    RoaringBitmap a = new RoaringBitmap();

    a.add(50000000);
    a.add(50000001);
    a.add(1111111111);

    int size = a.serializedSizeInBytes();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    a.serialize(buffer);

    buffer.flip();
    output.write(buffer);
    ChunkHeader chunkHeader = new ChunkHeader(size);
    long offset = output.write(chunkHeader);
    return new ChunkIndexEntry(offset, 3, 50000000, 0);
  }
}
