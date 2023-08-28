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

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;

public class TsFileIOWriterEndFileTest {
  public static void main(String[] args) throws Exception {
    try (TsFileIOWriter writer = new TsFileIOWriter(new File("test.tsfile"))) {
      for (int deviceIndex = 0; deviceIndex < 1000; deviceIndex++) {
        writer.startChunkGroup("root.sg.d" + deviceIndex);
        for (int seriesIndex = 0; seriesIndex < 1000; seriesIndex++) {
          ChunkWriterImpl chunkWriter =
              new ChunkWriterImpl(
                  new MeasurementSchema(
                      "s" + seriesIndex, TSDataType.INT32, TSEncoding.RLE, CompressionType.GZIP));
          for (long time = 0; time < 10; ++time) {
            chunkWriter.write(time, 0);
          }
          chunkWriter.writeToFileWriter(writer);
        }
        writer.endChunkGroup();
      }
      writer.endFile();
    }
  }
}
