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

package org.apache.iotdb.db.protocol.rest.v2;

import org.apache.iotdb.db.protocol.rest.v2.handler.StatementConstructionHandler;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertTabletRequest;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StatementConstructionTest {

  @Test
  public void testConstructInsertTabletStatement() {
    InsertTabletRequest insertTabletRequest = genInsertTabletRequest();
    try {
      StatementConstructionHandler.constructInsertTabletStatement(insertTabletRequest);
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testConstructInsertRowsStatement() {}

  private InsertTabletRequest genInsertTabletRequest() {
    List<String> measurements = new ArrayList<>();
    measurements.add("sensor0");
    measurements.add("sensor1");
    List<String> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.BOOLEAN.name());
    dataTypes.add(TSDataType.INT64.name());

    List<Long> times = new ArrayList<>();
    List<List<Object>> columns = new ArrayList<>();
    columns.add(new ArrayList<>());
    columns.add(new ArrayList<>());

    for (long r = 0; r < 101; r++) {
      times.add(r);
      columns.get(0).add(false);
      columns.get(1).add(r);
    }
    InsertTabletRequest insertTabletRequest = new InsertTabletRequest();
    insertTabletRequest.setTimestamps(times);
    insertTabletRequest.setValues(columns);
    insertTabletRequest.setDataTypes(dataTypes);
    insertTabletRequest.setMeasurements(measurements);
    insertTabletRequest.device("root.sg.d1");
    insertTabletRequest.setIsAligned(false);

    return insertTabletRequest;
  }
}
