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

package org.apache.tsfile.read;

import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnClosedTsFileReaderTest {

  @Test
  public void testRead() throws IOException {
    File file = new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "test.tsfile");
    TsFileIOWriter writer = new TsFileIOWriter(file);
    writer.startChunkGroup(Factory.DEFAULT_FACTORY.create("root.sg1.d1"));
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT64));
    chunkWriter.write(1, 1L);
    chunkWriter.write(2, 2L);
    chunkWriter.write(3, 3L);
    chunkWriter.writeToFileWriter(writer);
    writer.endChunkGroup();
    writer.close();
    ChunkMetadata chunkMetadata =
        writer.getChunkGroupMetadataList().get(0).getChunkMetadataList().get(0);

    UnClosedTsFileReader reader = new UnClosedTsFileReader(file.getAbsolutePath(), null);
    Chunk chunk = reader.readMemChunk(chunkMetadata);
    ChunkReader chunkReader = new ChunkReader(chunk);
    BatchData batchData = chunkReader.nextPageData();
    for (int i = 0; i < 3; i++) {
      assertTrue(batchData.hasCurrent());
      assertEquals(i + 1, batchData.currentTime());
      assertEquals(i + 1L, ((long) batchData.currentValue()));
      batchData.next();
    }
    assertFalse(batchData.hasCurrent());

    file.delete();
  }
}
