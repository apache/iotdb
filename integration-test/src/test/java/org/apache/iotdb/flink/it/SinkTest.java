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
package org.apache.iotdb.flink.it;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SinkTest extends AbstractTest {
  @Before
  @Override
  public void before() {
    super.before();
  }

  @Test
  public void testStreamingSink() {
    Schema dataGenTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("root.sg.d0.s0", DataTypes.FLOAT())
            .column("root.sg.d0.s1", DataTypes.FLOAT())
            .column("root.sg.d0.s2", DataTypes.FLOAT())
            .column("root.sg.d0.s3", DataTypes.FLOAT())
            .column("root.sg.d0.s4", DataTypes.FLOAT())
            .column("root.sg.d0.s5", DataTypes.FLOAT())
            .build();
    TableDescriptor descriptor =
        TableDescriptor.forConnector("datagen")
            .schema(dataGenTableSchema)
            .option("rows-per-second", "1")
            .option("fields.Time_.kind", "sequence")
            .option("fields.Time_.start", "1")
            .option("fields.Time_.end", "5")
            .option("fields.root.sg.d0.s0.min", "1")
            .option("fields.root.sg.d0.s0.max", "5")
            .option("fields.root.sg.d0.s1.min", "1")
            .option("fields.root.sg.d0.s1.max", "5")
            .option("fields.root.sg.d0.s2.min", "1")
            .option("fields.root.sg.d0.s2.max", "5")
            .option("fields.root.sg.d0.s3.min", "1")
            .option("fields.root.sg.d0.s3.max", "5")
            .option("fields.root.sg.d0.s4.min", "1")
            .option("fields.root.sg.d0.s4.max", "5")
            .option("fields.root.sg.d0.s5.min", "1")
            .option("fields.root.sg.d0.s5.max", "5")
            .build();
    tableEnv.createTemporaryTable("dataGenTable", descriptor);
    Table dataGenTable = tableEnv.from("dataGenTable");

    // create iotdb sink table
    TableDescriptor iotdbDescriptor =
        TableDescriptor.forConnector("IoTDB")
            .schema(dataGenTableSchema)
            .option("nodeUrls", String.format("%s:%d", ip, port))
            .build();
    tableEnv.createTemporaryTable("iotdbSinkTable", iotdbDescriptor);
    // insert data
    dataGenTable.executeInsert("iotdbSinkTable").print();
  }

  @Test
  public void testBatchSink() throws IoTDBConnectionException, StatementExecutionException {
    Utils.prepareData("root.sg.d0", ip, port);
    // schema
    Schema iotdbTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("root.sg.d0.s0", DataTypes.INT())
            .column("root.sg.d0.s1", DataTypes.BIGINT())
            .column("root.sg.d0.s2", DataTypes.FLOAT())
            .column("root.sg.d0.s3", DataTypes.DOUBLE())
            .column("root.sg.d0.s4", DataTypes.BOOLEAN())
            .column("root.sg.d0.s5", DataTypes.STRING())
            .build();

    // source table
    TableDescriptor sourceDescriptor =
        TableDescriptor.forConnector("IoTDB")
            .schema(iotdbTableSchema)
            .option("nodeUrls", String.format("%s:%d", ip, port))
            .option("sql", "select * from root.sg.d0")
            .build();
    tableEnv.createTemporaryTable("sourceTable", sourceDescriptor);
    Table sourceTable = tableEnv.from("sourceTable");

    // sink table
    TableDescriptor sinkDescriptor =
        TableDescriptor.forConnector("IoTDB")
            .schema(iotdbTableSchema)
            .option("nodeUrls", String.format("%s:%d", ip, port))
            .build();
    tableEnv.createTemporaryTable("sinkTable", sinkDescriptor);

    // insert data
    sourceTable.executeInsert("sinkTable").print();

    // read data from iotdb
    Session session = new Session.Builder().host(ip).port(port).build();
    session.open(false);

    SessionDataSet dataSet =
        session.executeQueryStatement("select s0,s1,s2,s3,s4,s5 from root.sg.d0");
    Object[] columnNames = dataSet.getColumnNames().toArray();
    Object[] columnTypes = dataSet.getColumnTypes().toArray();
    int rowSize = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      rowSize++;
    }

    Object[] exceptedColumnNames = {
      "Time",
      "root.sg.d0.s0",
      "root.sg.d0.s1",
      "root.sg.d0.s2",
      "root.sg.d0.s3",
      "root.sg.d0.s4",
      "root.sg.d0.s5"
    };
    Object[] exceptedColumnTypes = {
      "INT64", "INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "TEXT"
    };
    int exceptedRowSize = 1000;

    Assert.assertEquals(exceptedColumnNames, columnNames);
    Assert.assertEquals(exceptedColumnTypes, columnTypes);
    Assert.assertEquals(exceptedRowSize, rowSize);
    session.close();
  }

  @After
  @Override
  public void after() throws IoTDBConnectionException, StatementExecutionException {
    super.after();
  }
}
