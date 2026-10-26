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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.session.subscription.payload;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.TSRecord;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class TypeServicesTest {

  @Test
  public void testAppendTabletValuesToTsRecord() {
    TSRecord record = new TSRecord("root.sg.d", 1);

    TypeServices.TS_RECORD_VALUE_APPENDER_SERVICE
        .call(Type.fromTsDataType(TSDataType.INT32))
        .append(record, "int", new int[] {42}, 0);
    TypeServices.TS_RECORD_VALUE_APPENDER_SERVICE
        .call(Type.fromTsDataType(TSDataType.DATE))
        .append(record, "date", new LocalDate[] {LocalDate.of(2024, 8, 1)}, 0);
    TypeServices.TS_RECORD_VALUE_APPENDER_SERVICE
        .call(Type.fromTsDataType(TSDataType.BLOB))
        .append(record, "nullBlob", new Binary[] {null}, 0);

    Assert.assertEquals(2, record.dataPointList.size());
    Assert.assertEquals(42, record.dataPointList.get(0).getValue());
    Assert.assertEquals(20240801, record.dataPointList.get(1).getValue());
  }

  @Test
  public void testReadTabletValuesIntoField() {
    Field dateField = new Field(TSDataType.DATE);
    TypeServices.FIELD_VALUE_READER_SERVICE
        .call(Type.fromTsDataType(TSDataType.DATE))
        .read(dateField, new LocalDate[] {LocalDate.of(2024, 8, 1)}, 0);
    Assert.assertEquals(
        DateUtils.parseDateExpressionToInt(LocalDate.of(2024, 8, 1)).intValue(),
        dateField.getIntV());

    Field blobField = new Field(TSDataType.BLOB);
    TypeServices.FIELD_VALUE_READER_SERVICE
        .call(Type.fromTsDataType(TSDataType.BLOB))
        .read(blobField, new Binary[] {new Binary(new byte[] {1, 2, 3})}, 0);
    Assert.assertArrayEquals(new byte[] {1, 2, 3}, blobField.getBinaryV().getValues());
  }
}
