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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SchemaUtilsTest {
  @Test
  public void getAggregatedDataTypesTest() {
    List<TSDataType> measurementTypes = new ArrayList<>();
    measurementTypes.add(TSDataType.INT64);
    measurementTypes.add(TSDataType.TEXT);
    measurementTypes.add(TSDataType.BOOLEAN);
    measurementTypes.add(TSDataType.DOUBLE);
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.INT64),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.MIN_TIME));
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.INT64),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.COUNT));
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.DOUBLE),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.SUM));
    Assert.assertEquals(
        measurementTypes,
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.LAST_VALUE));
    Assert.assertEquals(
        measurementTypes,
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.MAX_VALUE));
  }

  @Test
  public void getSeriesTypeByPath() {
    Assert.assertEquals(
        TSDataType.DOUBLE, SchemaUtils.getSeriesTypeByPath(TSDataType.INT64, SqlConstant.SUM));
    Assert.assertEquals(
        TSDataType.INT64,
        SchemaUtils.getSeriesTypeByPath(TSDataType.INT64, SqlConstant.LAST_VALUE));
  }

  @Test
  public void checkDataTypeWithEncoding() {
    try {
      SchemaUtils.checkDataTypeWithEncoding(TSDataType.TEXT, TSEncoding.RLE);
      Assert.fail("expect exception");
    } catch (MetadataException e) {
      // do nothing
    }
  }
}
