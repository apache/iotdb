/**
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * Delete this class when submitting pr.
 */
public class PerformanceTest {

  private static int deviceStart = 9, deviceEnd = 9;
  private static int sensorStart = 9, sensorEnd = 9;

  public static void main(String[] args) throws IOException, FileNodeManagerException {

    // singleWithoutFilterTest();

    // queryMultiSeriesWithoutFilterTest();

    queryMultiSeriesWithFilterTest();
  }

  private static void singleWithoutFilterTest() throws IOException, FileNodeManagerException {

    List<Path> selectedPathList = new ArrayList<>();
    selectedPathList.add(getPath(1, 1));

    QueryExpression queryExpression = QueryExpression.create(selectedPathList, null);

    EngineQueryRouter queryRouter = new EngineQueryRouter();

    long startTime = System.currentTimeMillis();

    QueryDataSet queryDataSet = queryRouter.query(queryExpression);

    int count = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      count++;
      // output(count, rowRecord, true);
    }

    long endTime = System.currentTimeMillis();
    System.out
        .println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));

  }

  public static void queryMultiSeriesWithoutFilterTest()
      throws IOException, FileNodeManagerException {

    List<Path> selectedPathList = new ArrayList<>();
    for (int i = deviceStart; i <= deviceEnd; i++) {
      for (int j = sensorStart; j <= sensorEnd; j++) {
        selectedPathList.add(getPath(i, j));
      }
    }

    QueryExpression queryExpression = QueryExpression.create(selectedPathList, null);

    EngineQueryRouter queryRouter = new EngineQueryRouter();

    long startTime = System.currentTimeMillis();

    QueryDataSet queryDataSet = queryRouter.query(queryExpression);

    int count = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      count++;
    }

    long endTime = System.currentTimeMillis();
    System.out
        .println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));

  }

  public static void queryMultiSeriesWithFilterTest() throws IOException, FileNodeManagerException {

    List<Path> selectedPathList = new ArrayList<>();
    for (int i = deviceStart; i <= deviceEnd; i++) {
      for (int j = sensorStart; j <= sensorEnd; j++) {
        selectedPathList.add(getPath(i, j));
      }
    }

    Filter valueFilter = ValueFilter.gtEq(34300.0);
    Filter timeFilter = FilterFactory
        .and(TimeFilter.gtEq(1536396840000L), TimeFilter.ltEq(1537736665000L));

    IExpression expression = new SingleSeriesExpression(getPath(9, 9), timeFilter);
    EngineQueryRouter queryRouter = new EngineQueryRouter();

    QueryExpression queryExpression = QueryExpression.create(selectedPathList, expression);
    long startTime = System.currentTimeMillis();

    QueryDataSet queryDataSet = queryRouter.query(queryExpression);

    int count = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      count++;
      // if (count % 10000 == 0)
      // System.out.println(rowRecord);
    }

    long endTime = System.currentTimeMillis();
    System.out
        .println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));

  }

  public static void output(int cnt, RowRecord rowRecord, boolean flag) {
    if (!flag) {
      return;
    }

    if (cnt % 10000 == 0) {
      System.out.println(cnt + " : " + rowRecord);
    }

    if (cnt > 97600) {
      System.out.println("----" + cnt + " : " + rowRecord);
    }
  }

  public static Path getPath(int d, int s) {
    return new Path(String.format("root.perform.group_0.d_%s.s_%s", d, s));
  }

}
