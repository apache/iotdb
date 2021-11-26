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
package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class IoTDBAggregationGroupByLevelIT {
    private static final double DELTA = 1e-6;
    protected static boolean enableSeqSpaceCompaction;
    protected static boolean enableUnseqSpaceCompaction;
    protected static boolean enableCrossSpaceCompaction;
    @BeforeClass
    public static void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.envSetUp();

        enableSeqSpaceCompaction =
                IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
        enableUnseqSpaceCompaction =
                IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
        enableCrossSpaceCompaction =
                IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
        IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
        IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
        IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);

        AlignedWriteUtil.insertData();
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            // TODO currently aligned data in memory doesn't support deletion, so we flush all data to
            // disk before doing deletion
            statement.execute("flush");
            statement.execute("SET STORAGE GROUP TO root.sg1");
            statement.execute("create aligned timeseries root.sg1.d1(s1 FLOAT encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY, s3 INT64)");
            for (int i = 1; i <=10 ; i++) {
                statement.execute(String.format("insert into root.sg2.d1(time, s1) aligned values(%d,%f)",i,i));
            }
            for (int i = 10; i <=20 ; i++) {
                statement.execute(String.format("insert into root.sg2.d1(time, s2) aligned values(%d,%d)",i,i));
            }
            for (int i = 20; i <=30 ; i++) {
                statement.execute(String.format("insert into root.sg2.d1(time, s3) aligned values(%d,%d)",i,i));
            }
            for (int i = 30; i <=40 ; i++) {
                statement.execute(String.format("insert into root.sg2.d1(time, s1, s2, s3) aligned values(%d,%f,%d,%d)",i,i,i,i));
            }
            statement.execute("flush");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
        IoTDBDescriptor.getInstance()
                .getConfig()
                .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
        IoTDBDescriptor.getInstance()
                .getConfig()
                .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
        EnvironmentUtils.cleanEnv();
    }
}
