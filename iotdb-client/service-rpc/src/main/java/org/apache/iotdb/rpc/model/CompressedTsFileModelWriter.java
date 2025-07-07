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

package org.apache.iotdb.rpc.model;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.DeviceTableModelWriter;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.writer.TsFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

public class CompressedTsFileModelWriter extends ModelWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedTsFileModelWriter.class);
  private static final int DEFAULT_CHUNK_NUMBER = 128 * 128;

  @Override
  byte[] write(float[] values, int width, int height) {
    try {

      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(1);
      String tableName = "t";
      MemoryTsFileOutput tsFileOutput = new MemoryTsFileOutput(60 * 1024 * 1024);

      TableSchema tableSchema =
          new TableSchema(
              tableName,
              Collections.singletonList(
                  new ColumnSchemaBuilder()
                      .name("v")
                      .dataType(TSDataType.FLOAT)
                      .category(ColumnCategory.FIELD)
                      .build()));
      try (ITsFileWriter writer = new DeviceTableModelWriter(tsFileOutput, tableSchema, 1)) {
        Tablet tablet =
            new Tablet(
                Collections.singletonList("v"),
                Collections.singletonList(TSDataType.FLOAT),
                DEFAULT_CHUNK_NUMBER);

        for (int i = 0; i < values.length; i++) {
          int row = tablet.getRowSize();
          tablet.addTimestamp(row, i);
          tablet.addValue(row, "v", values[i]);
          // write
          if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
            writer.write(tablet);
            tablet.reset();
          }
        }
        // write
        if (tablet.getRowSize() != 0) {
          writer.write(tablet);
          tablet.reset();
        }
      }
      return tsFileOutput.getByteBuffer().array();
    } catch (Exception e) {
      LOGGER.error("write tsfile failed", e);
    }
    return new byte[0];
  }

  private static class MemoryTsFileOutput implements TsFileOutput {

    private ByteArrayOutputStream baos;

    public MemoryTsFileOutput(int initialSize) {
      this.baos = new PublicBAOS(initialSize);
    }

    public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(baos.toByteArray());
    }

    @Override
    public void write(byte[] b) throws IOException {
      baos.write(b);
    }

    @Override
    public void write(byte b) throws IOException {
      baos.write(b);
    }

    @Override
    public void write(ByteBuffer b) throws IOException {
      baos.write(b.array());
    }

    @Override
    public long getPosition() throws IOException {
      return baos.size();
    }

    @Override
    public void close() throws IOException {
      baos.close();
    }

    @Override
    public OutputStream wrapAsStream() throws IOException {
      return baos;
    }

    @Override
    public void flush() throws IOException {
      baos.flush();
    }

    @Override
    public void truncate(long size) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void force() throws IOException {}
  }
}
