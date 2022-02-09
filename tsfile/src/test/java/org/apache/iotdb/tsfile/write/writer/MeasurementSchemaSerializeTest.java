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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class MeasurementSchemaSerializeTest {

  @Test
  public void deserializeFromByteBufferTest() throws IOException {
    UnaryMeasurementSchema standard =
        new UnaryMeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    standard.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    UnaryMeasurementSchema measurementSchema = UnaryMeasurementSchema.deserializeFrom(byteBuffer);
    assertEquals(standard, measurementSchema);
  }

  @Test
  public void deserializeFromInputStreamTest() throws IOException {
    UnaryMeasurementSchema standard =
        new UnaryMeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    standard.serializeTo(byteBuffer);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(byteBuffer.array());
    UnaryMeasurementSchema measurementSchema = UnaryMeasurementSchema.deserializeFrom(inputStream);
    assertEquals(standard, measurementSchema);
  }
}
