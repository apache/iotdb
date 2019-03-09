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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.jdbc.Config;
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

  //  private static int deviceStart = 5, deviceEnd = 9;
  private static int deviceStart = 9, deviceEnd = 9;
  private static int sensorStart = 8, sensorEnd = 8;
  private static String insertTemplate = "INSERT INTO root.perform.group_0.d_%s(timestamp,s_%s"
      + ") VALUES(%d,%d)";

  public static void main(String[] args) throws IOException, FileNodeManagerException, SQLException {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
     //insert();

    //singleWithoutFilterTest();

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

    Filter valueFilter = ValueFilter.gtEq(34300);
    Filter timeFilter = FilterFactory
        .and(TimeFilter.gtEq(50000L), TimeFilter.ltEq(100000L));
    Filter filter = FilterFactory.and(timeFilter, valueFilter);

    IExpression expression = new SingleSeriesExpression(getPath(9, 9), filter);
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
        .println(String.format("Time consume : %s ms, count number : %s", endTime - startTime, count));

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

  private static void insert() throws SQLException {
    int d_start = 0, d_end = 10;
    int s_start = 0, s_end = 10;
    int num = 100000;
    prepareSeries(d_start,d_end,s_start,s_end);
    prepareData(d_start,d_end,s_start,s_end, num);
  }

  private static void prepareData(int d_start, int d_end, int s_start, int s_end, int num) throws SQLException {
    System.out.println("prepareData start!");
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
              "root");
      Statement statement = connection.createStatement();
      // prepare BufferWrite file
      for (int i = 20000; i <= 30000; i++) {
        insertInTimestamp(statement, d_start,d_end,s_start,s_end, i);
      }
      statement.execute("merge");
      System.out.println("prepareData 20000-30000 end ");

      // prepare Unseq-File
      for (int i = 1; i <= 10000; i++) {
        insertInTimestamp(statement, d_start,d_end,s_start,s_end, i);
      }
      statement.execute("merge");
      System.out.println("prepareData 1-10000 end ");

      // prepare muilty BufferWrite file
      for (int i = 40000; i <= 65000; i++) {
        insertInTimestamp(statement, d_start,d_end,s_start,s_end, i);
      }
      statement.execute("merge");
      System.out.println("prepareData 40000-65000 end ");

      // prepare muilty BufferWrite file
      for (int i = 80000; i <= 87000; i++) {
        insertInTimestamp(statement, d_start,d_end,s_start,s_end, i);
      }
      statement.execute("merge");
      System.out.println("prepareData 80000-87000 end ");

      // prepare BufferWrite cache
      for (int i = 90001; i <= 100000; i++) {
        insertInTimestamp(statement, d_start,d_end,s_start,s_end, i);
      }
      System.out.println("prepareData 90001-100000 end ");

      // prepare Overflow cache
      for (int i = 10001; i <= 20000; i++) {
        insertInTimestamp(statement, d_start,d_end,s_start,s_end, i);
      }
      System.out.println("prepareData 10001-20000 end ");
      statement.close();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  private static void insertInTimestamp(Statement statement,int d_start, int d_end, int s_start, int s_end, int time)
      throws SQLException {
    for(int m = d_start; m <= d_end; m++ ){
      for(int n = s_start; n<= s_end; n++){
        statement.execute(String.format(insertTemplate, m, n, time, time));
      }
    }
  }

  private static void prepareSeries(int d_start, int d_end, int s_start, int s_end) throws SQLException {
    System.out.println("prepareSeries start!");
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
              "root");
      Statement statement = connection.createStatement();
      for(int i = d_start; i <= d_end; i++ ){
        statement.execute(createStorageGroupSql(i));
      }
      statement.close();

      statement = connection.createStatement();
      for(int i = d_start; i <= d_end; i++ ){
        for(int j = s_start; j<= s_end; j++){
          statement.execute(String.format("CREATE TIMESERIES root.perform.group_0.d_%s.s_%s WITH DATATYPE=INT32, ENCODING=RLE", i, j));//
        }
      }
      statement.close();

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  public static String createStorageGroupSql(int d){
    return String.format("SET STORAGE GROUP TO root.perform.group_0.d_%s", d);
  }

}
