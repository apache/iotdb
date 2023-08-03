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
package org.apache.iotdb.jdbc;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class GroupedLSBWatermarkEncoderTest {

  @Test
  public void testEncodeRecord() {
    RowRecord rowRecord = new RowRecord(1000);
    Field field1 = new Field(TSDataType.INT32);
    field1.setIntV(1);
    rowRecord.addField(field1);
    Field field2 = new Field(TSDataType.INT64);
    field2.setLongV(1);
    rowRecord.addField(field2);
    Field field3 = new Field(TSDataType.FLOAT);
    field3.setFloatV(1.0f);
    rowRecord.addField(field3);
    Field field4 = new Field(TSDataType.DOUBLE);
    field4.setDoubleV(1.0d);
    rowRecord.addField(field4);
    Field field5 = new Field(TSDataType.BOOLEAN);
    field5.setBoolV(true);
    rowRecord.addField(field5);
    Field field6 = new Field(TSDataType.TEXT);
    field6.setBinaryV(new Binary("a"));
    rowRecord.addField(field6);
    GroupedLSBWatermarkEncoder groupedLSBWatermarkEncoder =
        new GroupedLSBWatermarkEncoder("a", "b", 1, 2);
    RowRecord record = groupedLSBWatermarkEncoder.encodeRecord(rowRecord);
    List<Field> fields = record.getFields();
    assertEquals(fields.get(0).getIntV(), 1);
    assertEquals(fields.get(1).getLongV(), 1);
    assertEquals(fields.get(2).getFloatV(), 1.0f, 0.1f);
    assertEquals(fields.get(3).getDoubleV(), 1.0d, 0.1d);
    assertEquals(fields.get(4).getBoolV(), true);
    assertEquals(fields.get(5).getBinaryV().getStringValue(), "a");
  }
}
