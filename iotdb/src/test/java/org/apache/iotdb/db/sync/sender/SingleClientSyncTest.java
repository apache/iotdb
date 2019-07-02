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
package org.apache.iotdb.db.sync.sender;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.integration.Constant;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.conf.Constans;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The test is to run a complete sync function Before you run the test, make sure receiver has been
 * cleaned up and inited.
 */
public class SingleClientSyncTest {

  SyncSenderImpl fileSenderImpl = SyncSenderImpl.getInstance();
  private IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
  private String serverIpTest = "192.168.130.7";
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private Set<String> dataSender = new HashSet<>();
  private Set<String> dataReceiver = new HashSet<>();
  private boolean success = true;
  private IoTDB deamon;
  private static final String[] sqls1 = new String[]{"SET STORAGE GROUP TO root.vehicle",
      "SET STORAGE GROUP TO root.test",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d1.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "insert into root.vehicle.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
      "insert into root.vehicle.d0(timestamp,s1) values(19,'103')",
      "insert into root.vehicle.d1(timestamp,s2) values(11,104.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(15,105.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(17,false)",
      "insert into root.vehicle.d0(timestamp,s0) values(20,1000)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(22,1001,'1002')",
      "insert into root.vehicle.d0(timestamp,s1) values(29,'1003')",
      "insert into root.vehicle.d1(timestamp,s2) values(21,1004.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(25,1005.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(27,true)",
      "insert into root.test.d0(timestamp,s0) values(10,106)",
      "insert into root.test.d0(timestamp,s0,s1) values(14,107,'108')",
      "insert into root.test.d0(timestamp,s1) values(16,'109')",
      "insert into root.test.d1.g0(timestamp,s0) values(1,110)",
      "insert into root.test.d0(timestamp,s0) values(30,1006)",
      "insert into root.test.d0(timestamp,s0,s1) values(34,1007,'1008')",
      "insert into root.test.d0(timestamp,s1) values(36,'1090')",
      "insert into root.test.d1.g0(timestamp,s0) values(10,1100)", "merge", "flush",};
  private static final String[] sqls2 = new String[]{
      "insert into root.vehicle.d0(timestamp,s0) values(6,120)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(38,121,'122')",
      "insert into root.vehicle.d0(timestamp,s1) values(9,'123')",
      "insert into root.vehicle.d0(timestamp,s0) values(16,128)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(18,189,'198')",
      "insert into root.vehicle.d0(timestamp,s1) values(99,'1234')",
      "insert into root.vehicle.d1(timestamp,s2) values(14,1024.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(29,1205.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(33,true)",
      "insert into root.test.d0(timestamp,s0) values(15,126)",
      "insert into root.test.d0(timestamp,s0,s1) values(8,127,'128')",
      "insert into root.test.d0(timestamp,s1) values(20,'129')",
      "insert into root.test.d1.g0(timestamp,s0) values(14,430)",
      "insert into root.test.d0(timestamp,s0) values(150,426)",
      "insert into root.test.d0(timestamp,s0,s1) values(80,427,'528')",
      "insert into root.test.d0(timestamp,s1) values(2,'1209')",
      "insert into root.test.d1.g0(timestamp,s0) values(4,330)", "merge", "flush",};
  private static final String[] sqls3 = new String[]{"SET STORAGE GROUP TO root.iotdb",
      "SET STORAGE GROUP TO root.flush",
      "CREATE TIMESERIES root.iotdb.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.iotdb.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.iotdb.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.flush.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.flush.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.flush.d1.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "insert into root.iotdb.d0(timestamp,s0) values(3,100)",
      "insert into root.iotdb.d0(timestamp,s0,s1) values(22,101,'102')",
      "insert into root.iotdb.d0(timestamp,s1) values(24,'103')",
      "insert into root.iotdb.d1(timestamp,s2) values(21,104.0)",
      "insert into root.iotdb.d1(timestamp,s2,s3) values(25,105.0,true)",
      "insert into root.iotdb.d1(timestamp,s3) values(27,false)",
      "insert into root.iotdb.d0(timestamp,s0) values(30,1000)",
      "insert into root.iotdb.d0(timestamp,s0,s1) values(202,101,'102')",
      "insert into root.iotdb.d0(timestamp,s1) values(44,'103')",
      "insert into root.iotdb.d1(timestamp,s2) values(1,404.0)",
      "insert into root.iotdb.d1(timestamp,s2,s3) values(250,10.0,true)",
      "insert into root.iotdb.d1(timestamp,s3) values(207,false)",
      "insert into root.flush.d0(timestamp,s0) values(20,106)",
      "insert into root.flush.d0(timestamp,s0,s1) values(14,107,'108')",
      "insert into root.flush.d1.g0(timestamp,s0) values(1,110)",
      "insert into root.flush.d0(timestamp,s0) values(200,1006)",
      "insert into root.flush.d0(timestamp,s0,s1) values(1004,1007,'1080')",
      "insert into root.flush.d1.g0(timestamp,s0) values(1000,910)",
      "insert into root.vehicle.d0(timestamp,s0) values(209,130)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(206,131,'132')",
      "insert into root.vehicle.d0(timestamp,s1) values(70,'33')",
      "insert into root.vehicle.d1(timestamp,s2) values(204,14.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(29,135.0,false)",
      "insert into root.vehicle.d1(timestamp,s3) values(14,false)",
      "insert into root.test.d0(timestamp,s0) values(19,136)",
      "insert into root.test.d0(timestamp,s0,s1) values(7,137,'138')",
      "insert into root.test.d0(timestamp,s1) values(30,'139')",
      "insert into root.test.d1.g0(timestamp,s0) values(4,150)",
      "insert into root.test.d0(timestamp,s0) values(1900,1316)",
      "insert into root.test.d0(timestamp,s0,s1) values(700,1307,'1038')",
      "insert into root.test.d0(timestamp,s1) values(3000,'1309')",
      "insert into root.test.d1.g0(timestamp,s0) values(400,1050)", "merge", "flush",};
  private boolean testFlag = Constant.testFlag;
  private static final String SYNC_CLIENT = Constans.SYNC_CLIENT;
  private static final Logger logger = LoggerFactory.getLogger(SingleClientSyncTest.class);

  public static void main(String[] args) throws Exception {
    SingleClientSyncTest singleClientPostBackTest = new SingleClientSyncTest();
    singleClientPostBackTest.setUp();
    singleClientPostBackTest.testPostback();
    singleClientPostBackTest.tearDown();
    System.exit(0);
  }

  public void setConfig() {
    config.setUuidPath(
        config.getDataDirectory() + SYNC_CLIENT + File.separator + Constans.UUID_FILE_NAME);
    config.setLastFileInfo(
        config.getDataDirectory() + SYNC_CLIENT + File.separator + Constans.LAST_LOCAL_FILE_NAME);
    String[] sequenceFileDirectory = config.getSeqFileDirectory();
    String[] snapshots = new String[config.getSeqFileDirectory().length];
    for (int i = 0; i < config.getSeqFileDirectory().length; i++) {
      sequenceFileDirectory[i] = new File(sequenceFileDirectory[i]).getAbsolutePath();
      if (!sequenceFileDirectory[i].endsWith(File.separator)) {
        sequenceFileDirectory[i] = sequenceFileDirectory[i] + File.separator;
      }
      snapshots[i] =
          sequenceFileDirectory[i] + SYNC_CLIENT + File.separator + Constans.DATA_SNAPSHOT_NAME
              + File.separator;
    }
    config.setSnapshotPaths(snapshots);
    config.setSeqFileDirectory(sequenceFileDirectory);
    config.setServerIp(serverIpTest);
    fileSenderImpl.setConfig(config);
  }

  public void setUp() throws StartupException, IOException {
    if (testFlag) {
      EnvironmentUtils.closeStatMonitor();
      deamon = IoTDB.getInstance();
      deamon.active();
      EnvironmentUtils.envSetUp();
    }
    setConfig();
  }

  public void tearDown() throws Exception {
    if (testFlag) {
      deamon.stop();
      EnvironmentUtils.cleanEnv();
    }
    if (success) {
      logger.debug("Test succeed!");
    } else {
      logger.debug("Test failed!");
    }
  }

  public void testPostback() throws IOException, SyncConnectionException, ClassNotFoundException, SQLException, InterruptedException {
    if (testFlag) {
      // the first time to sync
      logger.debug("It's the first time to sync!");
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                "root")) {
          Statement statement = connection.createStatement();
          for (String sql : sqls1) {
            statement.execute(sql);
          }
          statement.close();
        }
      } catch (SQLException | ClassNotFoundException e) {
        fail(e.getMessage());
      }

      fileSenderImpl.sync();

      // Compare data of sender and receiver
      dataSender.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                "root")) {
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("select * from root.vehicle");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
            }
          }
          hasResultSet = statement.execute("select * from root.test");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        }
      } catch (ClassNotFoundException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      dataReceiver.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                  "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("select * from root.vehicle");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
            }
          }

          hasResultSet = statement.execute("select * from root.test");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (ClassNotFoundException | SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }
      logger.debug(String.valueOf(dataSender.size()));
      logger.debug(String.valueOf(dataReceiver.size()));
      logger.debug(dataSender.toString());
      logger.debug(dataReceiver.toString());
      if (!(dataSender.size() == dataReceiver.size() && dataSender.containsAll(dataReceiver))) {
        success = false;
        return;
      }

      // the second time to sync
      logger.debug("It's the second time to sync!");
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                  "root");
          Statement statement = connection.createStatement();
          for (String sql : sqls2) {
            statement.execute(sql);
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (ClassNotFoundException | SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      fileSenderImpl.sync();

      // Compare data of sender and receiver
      dataSender.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                  "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("select * from root.vehicle");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
            }
          }
          hasResultSet = statement.execute("select * from root.test");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (ClassNotFoundException | SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      dataReceiver.clear();
      {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                  "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("select * from root.vehicle");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
            }
          }
          hasResultSet = statement.execute("select * from root.test");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      }
      logger.debug(String.valueOf(dataSender.size()));
      logger.debug(String.valueOf(dataReceiver.size()));
      logger.debug(dataSender.toString());
      logger.debug(dataReceiver.toString());
      if (!(dataSender.size() == dataReceiver.size() && dataSender.containsAll(dataReceiver))) {
        success = false;
        return;
      }

      // the third time to sync
      logger.debug("It's the third time to sync!");
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                "root")) {
          Statement statement = connection.createStatement();
          for (String sql : sqls3) {
            statement.execute(sql);
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        }
      } catch (ClassNotFoundException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      fileSenderImpl.sync();

      // Compare data of sender and receiver
      dataSender.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection = DriverManager
            .getConnection(String.format("jdbc:iotdb://%s:6667/", serverIpTest), "root",
                "root")) {
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("select * from root.vehicle");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
            }
          }
          hasResultSet = statement.execute("select * from root.test");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
            }
          }
          hasResultSet = statement.execute("select * from root.flush");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.flush.d0.s0")
                  + res.getString("root.flush.d0.s1") + res.getString("root.flush.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.flush.d0.s0")
                  + res.getString("root.flush.d0.s1") + res.getString("root.flush.d1.g0.s0"));
            }
          }
          hasResultSet = statement.execute("select * from root.iotdb");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataSender.add(res.getString("Time") + res.getString("root.iotdb.d0.s0")
                  + res.getString("root.iotdb.d0.s1") + res.getString("root.iotdb.d1.s2")
                  + res.getString("root.iotdb.d1.s3"));
              logger.debug(res.getString("Time") + res.getString("root.iotdb.d0.s0")
                  + res.getString("root.iotdb.d0.s1") + res.getString("root.iotdb.d1.s2")
                  + res.getString("root.iotdb.d1.s3"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        }
      } catch (ClassNotFoundException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }

      dataReceiver.clear();
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
          connection = DriverManager
              .getConnection("jdbc:iotdb://192.168.130.8:6667/", "root", "root");
          Statement statement = connection.createStatement();
          boolean hasResultSet = statement.execute("select * from root.vehicle");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
                  + res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
                  + res.getString("root.vehicle.d1.s3"));
            }
          }
          hasResultSet = statement.execute("select * from root.test");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.test.d0.s0")
                  + res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
            }
          }
          hasResultSet = statement.execute("select * from root.flush");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.flush.d0.s0")
                  + res.getString("root.flush.d0.s1") + res.getString("root.flush.d1.g0.s0"));
              logger.debug(res.getString("Time") + " | " + res.getString("root.flush.d0.s0")
                  + res.getString("root.flush.d0.s1") + res.getString("root.flush.d1.g0.s0"));
            }
          }
          hasResultSet = statement.execute("select * from root.iotdb");
          if (hasResultSet) {
            ResultSet res = statement.getResultSet();
            while (res.next()) {
              dataReceiver.add(res.getString("Time") + res.getString("root.iotdb.d0.s0")
                  + res.getString("root.iotdb.d0.s1") + res.getString("root.iotdb.d1.s2")
                  + res.getString("root.iotdb.d1.s3"));
              logger.debug(res.getString("Time") + res.getString("root.iotdb.d0.s0")
                  + res.getString("root.iotdb.d0.s1") + res.getString("root.iotdb.d1.s2")
                  + res.getString("root.iotdb.d1.s3"));
            }
          }
          statement.close();
        } catch (Exception e) {
          logger.error("", e);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      } catch (ClassNotFoundException | SQLException e) {
        fail(e.getMessage());
        Thread.currentThread().interrupt();
      }
      logger.debug(String.valueOf(dataSender.size()));
      logger.debug(String.valueOf(dataReceiver.size()));
      logger.debug(String.valueOf(dataSender));
      logger.debug(String.valueOf(dataReceiver));
      if (!(dataSender.size() == dataReceiver.size() && dataSender.containsAll(dataReceiver))) {
        success = false;
      }
    }
  }
}
