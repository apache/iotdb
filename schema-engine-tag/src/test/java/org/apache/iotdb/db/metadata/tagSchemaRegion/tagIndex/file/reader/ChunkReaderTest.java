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

import org.apache.iotdb.lsm.sstable.fileIO.FileInput;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class ChunkReaderTest {

  File file;

  ChunkReader chunkReader;

  @Before
  public void setUp() throws Exception {
    file = new File("testReadRoaringBitmap");
    serializeChunk(file);
    FileInput dataInput = new FileInput(file);
    chunkReader = new ChunkReader(dataInput);
  }

  @After
  public void tearDown() throws Exception {
    chunkReader.close();
    chunkReader = null;
    file.delete();
  }

  @Test
  public void testReadRoaringBitmap() throws IOException {
    RoaringBitmap roaringBitmap = chunkReader.readRoaringBitmap();
    assertEquals(4100, roaringBitmap.getCardinality());
  }

  @Test
  public void testIterator() throws IOException {
    int count = 0;
    while (chunkReader.hasNext()) {
      int now = chunkReader.next();
      if (count < 4097) {
        assertEquals(count * 6, now);
      } else if (count == 4097) {
        assertEquals(100000, now);
      } else if (count == 4098) {
        assertEquals(50000000, now);
      } else {
        assertEquals(50000001, now);
      }
      count++;
    }
  }

  private void serializeChunk(File file) throws IOException {
    RoaringBitmap a = new RoaringBitmap();
    for (int i = 0; i < 4097; i++) {
      a.add(i * 6);
    }
    a.add(100000);
    a.add(50000000);
    a.add(50000001);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 10);
    a.serialize(byteBuffer);
    int size = byteBuffer.position();
    byteBuffer.clear();
    DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(file));
    outputStream.writeInt(size);
    a.serialize(outputStream);
    outputStream.close();
  }
}
