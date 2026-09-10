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

package org.apache.iotdb.rest.protocol.v2.handler;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableId;
import org.apache.iotdb.rest.protocol.model.ExecutionStatus;
import org.apache.iotdb.rest.protocol.v2.model.QueryDataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FastLastHandlerTest {

  private static final String DEVICE = "root.sg25.d1";

  @Test
  public void fillLastValueDataSetShouldReturnAllEntriesWhenUnderLimit() {
    Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> resultMap =
        newResultMap("s1", "s2", "s3");

    Response response = FastLastHandler.fillLastValueDataSet(resultMap, 5);

    assertTrue(response.getEntity() instanceof QueryDataSet);
    QueryDataSet dataSet = (QueryDataSet) response.getEntity();
    assertEquals(3, dataSet.getTimestamps().size());
    // [timeseries, valueList, dataTypeList], each holding one row per measurement
    assertEquals(3, dataSet.getValues().size());
    assertEquals(3, dataSet.getValues().get(0).size());
  }

  @Test
  public void fillLastValueDataSetShouldCapAtLimitAndReportError() {
    // Regression for the fastLastQuery cache-hit path: a warm cache with more entries than the
    // configured limit must NOT materialize the whole result into heap.
    Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> resultMap =
        newResultMap("s1", "s2", "s3", "s4", "s5");

    Response response = FastLastHandler.fillLastValueDataSet(resultMap, 2);

    assertTrue(response.getEntity() instanceof ExecutionStatus);
    ExecutionStatus status = (ExecutionStatus) response.getEntity();
    assertEquals(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode(), status.getCode().intValue());
  }

  @Test
  public void fillLastValueDataSetShouldSkipPlaceholderAndNullEntries() {
    Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> resultMap =
        newResultMap("s1", "placeholder", "nullValue", "s4");

    Response response = FastLastHandler.fillLastValueDataSet(resultMap, 10);

    assertTrue(response.getEntity() instanceof QueryDataSet);
    QueryDataSet dataSet = (QueryDataSet) response.getEntity();
    // Only the two real entries (s1, s4) are materialized; placeholder/null entries are skipped.
    assertEquals(2, dataSet.getTimestamps().size());
  }

  /** Builds a single-table/single-device resultMap; the special names insert sentinel entries. */
  private static Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>>
      newResultMap(String... measurementNames) {
    Map<String, Pair<TSDataType, TimeValuePair>> measurements = new HashMap<>();
    long timestamp = 100L;
    for (String name : measurementNames) {
      measurements.put(name, newEntry(name, timestamp++));
    }
    Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> resultMap =
        new HashMap<>();
    resultMap.put(
        new TableId("root", "sg25"),
        new HashMap<>(
            Collections.singletonMap(
                IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE), measurements)));
    return resultMap;
  }

  private static Pair<TSDataType, TimeValuePair> newEntry(String name, long timestamp) {
    if ("placeholder".equals(name)) {
      return new Pair<>(TSDataType.INT64, TableDeviceLastCache.PLACEHOLDER_EMPTY_COLUMN);
    }
    if ("nullValue".equals(name)) {
      return new Pair<>(TSDataType.INT64, new TimeValuePair(timestamp, null));
    }
    return new Pair<>(
        TSDataType.INT64,
        new TimeValuePair(timestamp, TsPrimitiveType.getByType(TSDataType.INT64, timestamp)));
  }
}
