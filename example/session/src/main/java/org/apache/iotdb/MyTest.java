// package org.apache.iotdb;/*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
// import org.apache.iotdb.db.conf.IoTDBConfig;
// import org.apache.iotdb.db.conf.IoTDBDescriptor;
// import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
// import org.apache.iotdb.db.utils.EnvironmentUtils;
// import org.apache.iotdb.jdbc.Config;
// import org.junit.AfterClass;
// import org.junit.BeforeClass;
// import org.junit.Test;
//
// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.Statement;
// import java.util.Locale;
//
// public class MyTest {
//    private static String[] creationSqls =
//            new String[]{
//                    "SET STORAGE GROUP TO root.vehicle.d0",
//                    "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=FLOAT, ENCODING=plain",
//            };
//
//    private final String d0s0 = "root.vehicle.d0.s0";
//
//    private static final String insertTemplate =
//            "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%f)";
//
//    private static final IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
//    private static int avgSeriesPointNumberThreshold;
//
//    @BeforeClass
//    public static void setUp() throws Exception {
//        avgSeriesPointNumberThreshold = ioTDBConfig.getAvgSeriesPointNumberThreshold();
//
//
// IoTDBDescriptor.getInstance().getConfig().setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
//
//        EnvironmentUtils.envSetUp();
//
//        Class.forName(Config.JDBC_DRIVER_NAME);
//    }
//
//    @AfterClass
//    public static void tearDown() throws Exception {
////    EnvironmentUtils.cleanEnv();
//        IoTDBDescriptor.getInstance()
//                .getConfig()
//                .setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
//
//        ioTDBConfig.setAvgSeriesPointNumberThreshold(avgSeriesPointNumberThreshold);
//    }
//
//    @Test
//    public void test1() {
//        prepareData1();
//    }
//
//    private static void prepareData1() {
//        try (Connection connection =
//                     DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//             Statement statement = connection.createStatement()) {
//
//            String[] creationSqls =
//                    new String[]{
//                            "SET STORAGE GROUP TO root.vehicle.d0",
//                            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=FLOAT,
// ENCODING=plain",
//                    };
//            for (String sql : creationSqls) {
//                statement.execute(sql);
//            }
//
//            String insertTemplate =
//                    "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%f)";
//
//            ioTDBConfig.setSeqTsFileSize(1024*1024*1024);// 1G
//            ioTDBConfig.setUnSeqTsFileSize(1024*1024*1024); // 1G
//            ioTDBConfig.setAvgSeriesPointNumberThreshold(10000); // this step cannot be omitted
//
//            for (int i = 1; i <= 50000; i++) {
//                statement.addBatch(String.format(Locale.ENGLISH, insertTemplate, i,
// Math.random()));
//            }
//            statement.executeBatch();
//            statement.clearBatch();
//            statement.execute("flush");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
// }
