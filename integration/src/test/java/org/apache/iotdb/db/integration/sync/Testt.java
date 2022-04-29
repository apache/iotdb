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
package org.apache.iotdb.db.integration.sync;

import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Testt {

    protected Statement senderStatement;
    protected Connection senderConnection;
    @Before
    public void setup() throws Exception{
        Class.forName(Config.JDBC_DRIVER_NAME);
        senderConnection =
                DriverManager.getConnection(
                        "jdbc:iotdb://localhost:6668", "root", "root");
        senderStatement = senderConnection.createStatement();
    }
    @After
    public void clean() throws Exception{
        senderStatement.close();
        senderConnection.close();
    }

    private void prepareSchema() throws Exception {
        senderStatement.execute("set storage group to root.sg1");
        senderStatement.execute("set storage group to root.sg2");
        senderStatement.execute("create timeseries root.sg1.d1.s1 with datatype=int32, encoding=PLAIN");
        senderStatement.execute("create timeseries root.sg1.d1.s2 with datatype=float, encoding=RLE");
        senderStatement.execute("create timeseries root.sg1.d1.s3 with datatype=TEXT, encoding=PLAIN");
        senderStatement.execute("create timeseries root.sg1.d2.s4 with datatype=int64, encoding=PLAIN");
        senderStatement.execute("create timeseries root.sg2.d1.s0 with datatype=int32, encoding=PLAIN");
        senderStatement.execute(
                "create timeseries root.sg2.d2.s1 with datatype=boolean, encoding=PLAIN");
    }

    /* add one seq tsfile in sg1 */
    private void prepareIns1() throws Exception {
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(1, 1, 16.0, 'a')");
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(2, 2, 25.16, 'b')");
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(3, 3, 65.25, 'c')");
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(16, 25, 100.0, 'd')");
        senderStatement.execute("insert into root.sg1.d2(timestamp, s4) values(1, 1)");
        senderStatement.execute("flush");
    }

    /* add one seq tsfile in sg1 */
    private void prepareIns2() throws Exception {
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(100, 65, 16.25, 'e')");
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(65, 100, 25.0, 'f')");
        senderStatement.execute("insert into root.sg1.d2(timestamp, s4) values(200, 100)");
        senderStatement.execute("flush");
    }

    /* add one seq tsfile in sg1, one unseq tsfile in sg1, one seq tsfile in sg2 */
    private void prepareIns3() throws Exception {
        senderStatement.execute("insert into root.sg2.d1(timestamp, s0) values(100, 100)");
        senderStatement.execute("insert into root.sg2.d1(timestamp, s0) values(65, 65)");
        senderStatement.execute("insert into root.sg2.d2(timestamp, s1) values(1, true)");
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(25, 16, 65.16, 'g')");
        senderStatement.execute(
                "insert into root.sg1.d1(timestamp, s1, s2, s3) values(200, 100, 16.65, 'h')");
        senderStatement.execute("flush");
    }

    private void prepareDel1() throws Exception { // after ins1, add 2 deletions
        senderStatement.execute("delete from root.sg1.d1.s1 where time == 3");
        senderStatement.execute("delete from root.sg1.d1.s2 where time >= 1 and time <= 2");
    }

    private void prepareDel2() throws Exception { // after ins2, add 3 deletions
        senderStatement.execute("delete from root.sg1.d1.s3 where time <= 65");
    }

    private void prepareDel3() throws Exception { // after ins3, add 5 deletions, 2 schemas{
        senderStatement.execute("delete from root.sg1.d1.* where time <= 2");
        senderStatement.execute("delete timeseries root.sg1.d2.*");
        senderStatement.execute("delete storage group root.sg2");
    }

    @Test
    public void run() throws Exception{
        prepareSchema();
        prepareIns1();
        prepareIns2();
        prepareIns3();
        prepareDel1();
        prepareDel2();
        prepareDel3();
    }
}
