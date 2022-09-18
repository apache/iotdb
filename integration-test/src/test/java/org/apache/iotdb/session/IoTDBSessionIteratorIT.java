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
package org.apache.iotdb.session;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionIteratorIT {

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  private void prepareData(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    session.setStorageGroup("root.sg1");
    session.createTimeseries(
        "root.sg1.d1.s1", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s3", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s4", TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s5", TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s6", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s1", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      measurements.add("s" + i);
      types.add(TSDataType.deserialize((byte) (i - 1)));
    }

    for (long time = 0; time < 10; time++) {
      List<Object> values = new ArrayList<>();
      values.add((time % 2) == 0);
      values.add((int) time);
      values.add(time * 10);
      values.add(time * 1.5f);
      values.add(time * 2.5);
      values.add("time" + time);
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    deviceId = "root.sg1.d2";
    measurements = new ArrayList<>();
    types = new ArrayList<>();
    measurements.add("s1");
    types.add(TSDataType.BOOLEAN);

    for (long time = 5; time < 10; time++) {
      List<Object> values = new ArrayList<>();
      values.add((time % 2) == 0);
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  @Test
  public void getValueByColumnIndexTest() {
    String[] retArray =
        new String[] {
          "0,true,0,0,0.0,0.0,time0",
          "1,false,1,10,1.5,2.5,time1",
          "2,true,2,20,3.0,5.0,time2",
          "3,false,3,30,4.5,7.5,time3",
          "4,true,4,40,6.0,10.0,time4",
          "5,false,5,50,7.5,12.5,time5",
          "6,true,6,60,9.0,15.0,time6",
          "7,false,7,70,10.5,17.5,time7",
          "8,true,8,80,12.0,20.0,time8",
          "9,false,9,90,13.5,22.5,time9",
        };

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select s1,s2,s3,s4,s5,s6 from root.sg1.d1");
      sessionDataSet.setFetchSize(1024);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        StringBuilder ans = new StringBuilder();
        ans.append(iterator.getLong(1))
            .append(",")
            .append(iterator.getBoolean(2))
            .append(",")
            .append(iterator.getInt(3))
            .append(",")
            .append(iterator.getLong(4))
            .append(",")
            .append(iterator.getFloat(5))
            .append(",")
            .append(iterator.getDouble(6))
            .append(",")
            .append(iterator.getString(7));
        assertEquals(retArray[count], ans.toString());
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void getValueByColumnNameTest() {
    String[] retArray =
        new String[] {
          "0,true,0,0,0.0,0.0,time0",
          "1,false,1,10,1.5,2.5,time1",
          "2,true,2,20,3.0,5.0,time2",
          "3,false,3,30,4.5,7.5,time3",
          "4,true,4,40,6.0,10.0,time4",
          "5,false,5,50,7.5,12.5,time5",
          "6,true,6,60,9.0,15.0,time6",
          "7,false,7,70,10.5,17.5,time7",
          "8,true,8,80,12.0,20.0,time8",
          "9,false,9,90,13.5,22.5,time9",
        };

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.sg1.d1");
      sessionDataSet.setFetchSize(1024);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        StringBuilder ans = new StringBuilder();
        ans.append(iterator.getLong("Time"))
            .append(",")
            .append(iterator.getBoolean("root.sg1.d1.s1"))
            .append(",")
            .append(iterator.getInt("root.sg1.d1.s2"))
            .append(",")
            .append(iterator.getLong("root.sg1.d1.s3"))
            .append(",")
            .append(iterator.getFloat("root.sg1.d1.s4"))
            .append(",")
            .append(iterator.getDouble("root.sg1.d1.s5"))
            .append(",")
            .append(iterator.getString("root.sg1.d1.s6"));
        assertEquals(retArray[count], ans.toString());
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void getObjectByColumnNameTest() {
    String[] retArray =
        new String[] {
          "0,true,0,0,0.0,0.0,time0",
          "1,false,1,10,1.5,2.5,time1",
          "2,true,2,20,3.0,5.0,time2",
          "3,false,3,30,4.5,7.5,time3",
          "4,true,4,40,6.0,10.0,time4",
          "5,false,5,50,7.5,12.5,time5",
          "6,true,6,60,9.0,15.0,time6",
          "7,false,7,70,10.5,17.5,time7",
          "8,true,8,80,12.0,20.0,time8",
          "9,false,9,90,13.5,22.5,time9",
        };
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select s1,s2,s3,s4,s5,s6 from root.sg1.d1");
      sessionDataSet.setFetchSize(1024);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        StringBuilder ans = new StringBuilder();
        long time = (long) iterator.getObject(1);
        boolean s1 = (boolean) iterator.getObject(2);
        int s2 = (int) iterator.getObject(3);
        long s3 = (long) iterator.getObject(4);
        float s4 = (float) iterator.getObject(5);
        double s5 = (double) iterator.getObject(6);
        String s6 = (String) iterator.getObject(7);
        ans.append(time)
            .append(",")
            .append(s1)
            .append(",")
            .append(s2)
            .append(",")
            .append(s3)
            .append(",")
            .append(s4)
            .append(",")
            .append(s5)
            .append(",")
            .append(s6);
        assertEquals(retArray[count], ans.toString());
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastQueryTest() {
    String[] retArray =
        new String[] {
          "9,root.sg1.d1.s3,90,INT64",
          "9,root.sg1.d1.s4,13.5,FLOAT",
          "9,root.sg1.d1.s5,22.5,DOUBLE",
          "9,root.sg1.d1.s6,time9,TEXT",
          "9,root.sg1.d1.s1,false,BOOLEAN",
          "9,root.sg1.d1.s2,9,INT32",
        };

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select last s1,s2,s3,s4,s5,s6 from root.sg1.d1");
      sessionDataSet.setFetchSize(1024);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        String ans =
            String.format(
                "%s,%s,%s,%s",
                iterator.getLong(1),
                iterator.getString(2),
                iterator.getString(3),
                iterator.getString(4));
        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void showDevicesTest() {
    String[] retArray = new String[] {"root.sg1.d1,false", "root.sg1.d2,false"};
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      SessionDataSet sessionDataSet = session.executeQueryStatement("show devices");
      sessionDataSet.setFetchSize(1024);
      assertEquals(2, sessionDataSet.getColumnNames().size());
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        String ans = String.format("%s,%s", iterator.getString(1), iterator.getString(2));
        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void queryWithTimeoutTest() {
    String[] retArray = new String[] {"9,root.sg1.d1.s1,false,BOOLEAN"};

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      prepareData(session);

      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select last s1 from root.sg1.d1", 2000);
      sessionDataSet.setFetchSize(1024);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        String ans =
            String.format(
                "%s,%s,%s,%s",
                iterator.getLong(1),
                iterator.getString(2),
                iterator.getString(3),
                iterator.getString(4));
        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
