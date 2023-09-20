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
package org.apache.iotdb.example;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class BatchSinkExample {
  public static void main(String[] args) {
    // setup environment
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    // create source table
    Schema sourceTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("root.sg.d0.s0", DataTypes.FLOAT())
            .column("root.sg.d1.s0", DataTypes.FLOAT())
            .column("root.sg.d1.s1", DataTypes.FLOAT())
            .build();
    TableDescriptor sourceTableDescriptor =
        TableDescriptor.forConnector("IoTDB")
            .schema(sourceTableSchema)
            .option("sql", "select ** from root.sg.d0,root.sg.d1")
            .build();

    tableEnv.createTemporaryTable("sourceTable", sourceTableDescriptor);
    Table sourceTable = tableEnv.from("sourceTable");
    // register sink table
    Schema sinkTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("root.sg.d2.s0", DataTypes.FLOAT())
            .column("root.sg.d3.s0", DataTypes.FLOAT())
            .column("root.sg.d3.s1", DataTypes.FLOAT())
            .build();
    TableDescriptor sinkTableDescriptor =
        TableDescriptor.forConnector("IoTDB").schema(sinkTableSchema).build();
    tableEnv.createTemporaryTable("sinkTable", sinkTableDescriptor);

    // insert data
    sourceTable
        .renameColumns(
            $("root.sg.d0.s0").as("root.sg.d2.s0"),
            $("root.sg.d1.s0").as("root.sg.d3.s0"),
            $("root.sg.d1.s1").as("root.sg.d3.s1"))
        .insertInto("sinkTable")
        .execute()
        .print();
  }
}
