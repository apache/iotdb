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

import org.apache.flink.table.api.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;

public class SourceTest extends AbstractTest {
  @Before
  public void before() {
    super.before();
  }

  @Test
  public void boundedScanTest() throws IoTDBConnectionException, StatementExecutionException {
    Utils.prepareData("root.test.flink.scan", ip, port);

    Schema iotdbTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("s0", DataTypes.INT())
            .column("s1", DataTypes.BIGINT())
            .column("s2", DataTypes.FLOAT())
            .column("s3", DataTypes.DOUBLE())
            .column("s4", DataTypes.BOOLEAN())
            .column("s5", DataTypes.STRING())
            .build();

    TableDescriptor iotdbDescriptor =
        TableDescriptor.forConnector("IoTDB")
            .schema(iotdbTableSchema)
            .option("nodeUrls", String.format("%s:%d", ip, port))
            .option("device", "root.test.flink.scan")
            .build();
    tableEnv.createTemporaryTable("iotdbTable", iotdbDescriptor);

    Table iotdbTable = tableEnv.from("iotdbTable");

    TableSchema schema = iotdbTable.getSchema();
    String[] fieldNames = schema.getFieldNames();
    DataType[] fieldDataTypes = schema.getFieldDataTypes();
    CloseableIterator<Row> collect = iotdbTable.execute().collect();
    int rowSize = 0;
    while (collect.hasNext()) {
      collect.next();
      rowSize ++;
    }

    final String[] exceptedFiledNames = {"Time_", "s0", "s1", "s2", "s3", "s4", "s5"};
    final DataType[] exceptedFieldDataTypes = {DataTypes.BIGINT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN(), DataTypes.STRING()};
    final int exceptedRowSize = 1000;

    Assert.assertEquals(exceptedFiledNames, fieldNames);
    Assert.assertEquals(exceptedFieldDataTypes, fieldDataTypes);
    Assert.assertEquals(exceptedRowSize, rowSize);
  }

  @Test
  public void lookupTest() throws IoTDBConnectionException, StatementExecutionException {
    Utils.prepareData("root.test.flink.lookup", ip, port);

    Schema dataGenTableSchema =
            Schema.newBuilder()
                    .column("Time_", DataTypes.BIGINT())
                    .column("s6", DataTypes.INT())
                    .build();

    TableDescriptor datagenDescriptor =TableDescriptor
            .forConnector("datagen")
            .schema(dataGenTableSchema)
            .option("fields.Time_.kind", "sequence")
            .option("fields.Time_.start", "1")
            .option("fields.Time_.end", "100")
            .option("fields.s6.min", "1")
            .option("fields.s6.max", "1")
            .build();
    tableEnv.createTemporaryTable("leftTable", datagenDescriptor);

    Schema iotdbTableSchema =
            Schema.newBuilder()
                    .column("Time_", DataTypes.BIGINT())
                    .column("s0", DataTypes.INT())
                    .column("s1", DataTypes.BIGINT())
                    .column("s2", DataTypes.FLOAT())
                    .column("s3", DataTypes.DOUBLE())
                    .column("s4", DataTypes.BOOLEAN())
                    .column("s5", DataTypes.STRING())
                    .build();

    TableDescriptor iotdbDescriptor =
            TableDescriptor.forConnector("IoTDB")
                    .schema(iotdbTableSchema)
                    .option("nodeUrls", String.format("%s:%d", ip, port))
                    .option("device", "root.test.flink.lookup")
                    .build();
    tableEnv.createTemporaryTable("rightTable", iotdbDescriptor);

    String sql = "SELECT l.Time_, r.s0, r.s1, r.s2, r.s3, r.s4, r.s5, l.s6 " +
            "FROM (select *,PROCTIME() as proc_time from leftTable) AS l " +
            "JOIN rightTable FOR SYSTEM_TIME AS OF l.proc_time AS r " +
            "ON l.Time_ = r.Time_";

    TableResult result = tableEnv.sqlQuery(sql).execute();
    TableSchema schema = result.getTableSchema();
    String[] fieldNames = schema.getFieldNames();
    DataType[] fieldDataTypes = schema.getFieldDataTypes();
    CloseableIterator<Row> collect = result.collect();
    int rowSize = 0;
    while (collect.hasNext()) {
      collect.next();
      rowSize ++;
    }

    final String[] exceptedFiledNames = {"Time_", "s0", "s1", "s2", "s3", "s4", "s5", "s6"};
    final DataType[] exceptedFieldDataTypes = {DataTypes.BIGINT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN(), DataTypes.STRING(), DataTypes.INT()};
    final int exceptedRowSize = 100;

    Assert.assertEquals(exceptedFiledNames, fieldNames);
    Assert.assertEquals(exceptedFieldDataTypes, fieldDataTypes);
    Assert.assertEquals(exceptedRowSize, rowSize);
  }

  @After
  public void after() throws IoTDBConnectionException {
    super.after();
  }
}
