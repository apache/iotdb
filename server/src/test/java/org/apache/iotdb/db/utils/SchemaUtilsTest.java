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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SchemaUtilsTest {
  @Test
  public void registerTimeseriesTest() throws MetadataException {
    MManager mmanager = IoTDB.metaManager;
    mmanager.init();

    String tsPath = "root.sg.d1.s1";
    TimeseriesSchema timeseriesSchema = new TimeseriesSchema(tsPath, TSDataType.INT32);
    SchemaUtils.registerTimeseries(timeseriesSchema);
    Assert.assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(tsPath)));
  }

  @Test
  public void getAggregatedDataTypesTest() {
    List<TSDataType> measurementTypes = new ArrayList<>();
    measurementTypes.add(TSDataType.INT64);
    measurementTypes.add(TSDataType.TEXT);
    measurementTypes.add(TSDataType.BOOLEAN);
    measurementTypes.add(TSDataType.DOUBLE);
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.INT64),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SQLConstant.MIN_TIME));
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.INT64),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SQLConstant.COUNT));
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.DOUBLE),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SQLConstant.SUM));
    Assert.assertEquals(
        measurementTypes,
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SQLConstant.LAST_VALUE));
    Assert.assertEquals(
        measurementTypes,
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SQLConstant.MAX_VALUE));
  }
}
