/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.session;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.encoding.table.ClusterTable;
import org.apache.tsfile.encoding.table.ClusterTableCodec;
import org.apache.tsfile.encoding.table.ClusterTableOptions;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterTableSessionTest {
  static ClusterTable sample(int n) {
    long[] times = new long[n];
    Object[][] rows = new Object[n][4];
    for (int r = 0; r < n; r++) {
      times[r] = 1700000000000L + (r % 97);
      rows[r][0] = (double) ((r * 17) % 31);
      rows[r][1] = ((Double) rows[r][0]) * 3.0;
      rows[r][2] = r % 11 == 0 ? -0.0 : Math.sin(r);
      rows[r][3] = r % 13 == 0 ? null : Long.MAX_VALUE - r;
    }
    return new ClusterTable(
        times,
        new String[] {"x", "y", "z", "counter"},
        new TSDataType[] {
          TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.INT64
        },
        rows);
  }

  static void records(Map<String, Integer> result, ClusterTable table) {
    for (int r = 0; r < table.rowCount(); r++) {
      StringBuilder key = new StringBuilder().append(table.timestamp(r));
      for (int c = 0; c < table.columnCount(); c++) {
        key.append('/').append(table.isNull(r, c) ? "null" : Long.toHexString(table.rawBits(r, c)));
      }
      result.merge(key.toString(), 1, Integer::sum);
    }
  }

  @Test
  public void rpcPayloadRoundTripAndOriginalTimeFilter() throws Exception {
    for (ClusterTableOptions options :
        Arrays.asList(ClusterTableOptions.aCluster(), ClusterTableOptions.kCluster())) {
      ISession session = mock(ISession.class);
      List<RowRecord> stored = new ArrayList<>();
      doAnswer(
              call -> {
                long id = call.getArgument(1);
                List<Object> values = call.getArgument(4);
                ClusterTable page = ClusterTableCodec.decode(((Binary) values.get(0)).getValues());
                assertEquals(page.rowCount(), values.get(3));
                long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
                for (int r = 0; r < page.rowCount(); r++) {
                  min = Math.min(min, page.timestamp(r));
                  max = Math.max(max, page.timestamp(r));
                }
                assertEquals(min, values.get(1));
                assertEquals(max, values.get(2));
                RowRecord row = new RowRecord(id);
                Field field = new Field(TSDataType.BLOB);
                field.setBinaryV((Binary) values.get(0));
                row.addField(field);
                stored.add(row);
                return null;
              })
          .when(session)
          .insertRecord(anyString(), anyLong(), anyList(), anyList(), anyList());
      when(session.executeQueryStatement(anyString()))
          .thenAnswer(
              call -> {
                SessionDataSet result = mock(SessionDataSet.class);
                Iterator<RowRecord> rows = stored.iterator();
                when(result.hasNext()).thenAnswer(ignore -> rows.hasNext());
                when(result.next()).thenAnswer(ignore -> rows.next());
                return result;
              });
      ClusterTableSession client = new ClusterTableSession(session, "root.cluster_test.a", options);
      ClusterTable table = sample(180);
      client.createSchema();
      client.writeTable(1, table);
      client.writeBlock(2, table.slice(0, 20));
      Map<String, Integer> expected = new HashMap<>(), actual = new HashMap<>();
      records(expected, table);
      records(expected, table.slice(0, 20));
      client.readAll(page -> records(actual, page));
      assertEquals(expected, actual);
      expected.clear();
      actual.clear();
      long start = 1700000000010L, end = 1700000000020L;
      records(expected, table.timeRange(start, end));
      records(expected, table.slice(0, 20).timeRange(start, end));
      client.readTimeRange(start, end, page -> records(actual, page));
      assertEquals(expected, actual);
      assertThrows(IllegalArgumentException.class, () -> client.writeBlock(2, table));
      assertThrows(
          IllegalArgumentException.class, () -> client.readTimeRange(end, start, page -> {}));
    }
  }

  @Test
  public void failedInsertCannotSilentlyAdvance() throws Exception {
    ISession session = mock(ISession.class);
    doThrow(new StatementExecutionException("unknown outcome"))
        .when(session)
        .insertRecord(anyString(), anyLong(), anyList(), anyList(), anyList());
    ClusterTableSession client =
        new ClusterTableSession(session, "root.cluster_test.a", ClusterTableOptions.aCluster());
    assertThrows(StatementExecutionException.class, () -> client.writeBlock(1, sample(2)));
    assertThrows(IOException.class, () -> client.writeBlock(2, sample(2)));
  }

  @Test
  public void rejectsUnsafeDevicePathAndEmptyBlocks() throws Exception {
    ISession session = mock(ISession.class);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ClusterTableSession(
                session, "root.a; delete timeseries root.**", ClusterTableOptions.aCluster()));
    ClusterTableSession client =
        new ClusterTableSession(session, "root.cluster_test.a", ClusterTableOptions.aCluster());
    assertThrows(IllegalArgumentException.class, () -> client.writeBlock(1, sample(0)));
  }
}
