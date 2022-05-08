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
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AlignedChunkWriterImplTest {

  @Test
  public void testWrite1() {
    VectorMeasurementSchemaStub measurementSchema = new VectorMeasurementSchemaStub();
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchema);

    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }

    chunkWriter.sealCurrentPage();
    // time chunk: 4(PageHeader Size: uncompressedSize + compressedSize) + 160(dataSize);
    // value chunk 1: 2(PageHeader Size: uncompressedSize + compressedSize) +
    // 4(bitmap length) + 3(bitmap data) + 80(data size);
    // value chunk 2: 2 + 4 + 3 + 20;
    // value chunk 3: 4 + 4 + 3 + 20 * 8;
    assertEquals(453L, chunkWriter.getSerializedChunkSize());
  }

  @Test
  public void testWrite2() {
    VectorMeasurementSchemaStub measurementSchema = new VectorMeasurementSchemaStub();
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(measurementSchema);

    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();
    for (int time = 21; time <= 40; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();

    // time chunk: (4 + 17 + 160) * 2
    // value chunk 1: (2 + 41 + 4 + 3 + 80) * 2
    // value chunk 2: (2 + 41 + 4 + 3 + 20) * 2
    // value chunk 3: (4 + 57 + 4 + 3 + 160) * 2
    assertEquals(1218L, chunkWriter.getSerializedChunkSize());
  }
}
