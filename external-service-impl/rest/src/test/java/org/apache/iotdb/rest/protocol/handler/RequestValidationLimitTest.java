/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rest.protocol.handler;

import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.rest.protocol.exception.RequestLimitExceededException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class RequestValidationLimitTest {

  private IoTDBRestServiceConfig config;
  private int originalMaxInsertRows;
  private int originalMaxInsertColumns;
  private long originalMaxInsertValues;

  @Before
  public void setUp() {
    config = IoTDBRestServiceDescriptor.getInstance().getConfig();
    originalMaxInsertRows = config.getRestMaxInsertRows();
    originalMaxInsertColumns = config.getRestMaxInsertColumns();
    originalMaxInsertValues = config.getRestMaxInsertValues();
  }

  @After
  public void tearDown() {
    config.setRestMaxInsertRows(originalMaxInsertRows);
    config.setRestMaxInsertColumns(originalMaxInsertColumns);
    config.setRestMaxInsertValues(originalMaxInsertValues);
  }

  @Test(expected = RequestLimitExceededException.class)
  public void testV1InsertTabletRejectsTooManyRows() {
    config.setRestMaxInsertRows(2);

    org.apache.iotdb.rest.protocol.v1.model.InsertTabletRequest request =
        new org.apache.iotdb.rest.protocol.v1.model.InsertTabletRequest();
    request.setDeviceId("root.sg.d1");
    request.setIsAligned(false);
    request.setMeasurements(Collections.singletonList("s1"));
    request.setDataTypes(Collections.singletonList("INT64"));
    request.setTimestamps(Arrays.asList(1L, 2L, 3L));
    request.setValues(Collections.singletonList(Arrays.asList(1L, 2L, 3L)));

    org.apache.iotdb.rest.protocol.v1.handler.RequestValidationHandler.validateInsertTabletRequest(
        request);
  }

  @Test(expected = RequestLimitExceededException.class)
  public void testV2InsertRecordsRejectsTooManyValues() {
    config.setRestMaxInsertRows(10);
    config.setRestMaxInsertColumns(10);
    config.setRestMaxInsertValues(2);

    org.apache.iotdb.rest.protocol.v2.model.InsertRecordsRequest request =
        new org.apache.iotdb.rest.protocol.v2.model.InsertRecordsRequest();
    request.setIsAligned(false);
    request.setDevices(Arrays.asList("root.sg.d1", "root.sg.d2"));
    request.setTimestamps(Arrays.asList(1L, 2L));
    request.setMeasurementsList(
        Arrays.asList(
            Arrays.asList("s1", "s2"),
            Collections.singletonList("s1")));
    request.setDataTypesList(
        Arrays.asList(
            Arrays.asList("INT64", "INT64"),
            Collections.singletonList("INT64")));
    request.setValuesList(Arrays.asList(Arrays.asList(1L, 2L), Collections.singletonList(3L)));

    org.apache.iotdb.rest.protocol.v2.handler.RequestValidationHandler.validateInsertRecordsRequest(
        request);
  }

  @Test(expected = RequestLimitExceededException.class)
  public void testTableInsertTabletRejectsTooManyColumns() {
    config.setRestMaxInsertColumns(1);

    org.apache.iotdb.rest.protocol.table.v1.model.InsertTabletRequest request =
        new org.apache.iotdb.rest.protocol.table.v1.model.InsertTabletRequest();
    request.setDatabase("db");
    request.setTable("t1");
    request.setColumnNames(Arrays.asList("tag1", "s1"));
    request.setColumnCategories(Arrays.asList("TAG", "FIELD"));
    request.setDataTypes(Arrays.asList("STRING", "INT64"));
    request.setTimestamps(Collections.singletonList(1L));
    request.setValues(Collections.singletonList(Arrays.asList("a", 1L)));

    org.apache.iotdb.rest.protocol.table.v1.handler.RequestValidationHandler
        .validateInsertTabletRequest(request);
  }
}
