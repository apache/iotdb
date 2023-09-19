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

public class StreamingSinkExample {
  public static void main(String[] args) {
    // setup environment
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    // create data source table
    Schema dataGenTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("root.sg.d0.s0", DataTypes.FLOAT())
            .column("root.sg.d1.s0", DataTypes.FLOAT())
            .column("root.sg.d1.s1", DataTypes.FLOAT())
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
            .option("fields.root.sg.d1.s0.min", "1")
            .option("fields.root.sg.d1.s0.max", "5")
            .option("fields.root.sg.d1.s1.min", "1")
            .option("fields.root.sg.d1.s1.max", "5")
            .build();
    // register source table
    tableEnv.createTemporaryTable("dataGenTable", descriptor);
    Table dataGenTable = tableEnv.from("dataGenTable");

    // create iotdb sink table
    TableDescriptor iotdbDescriptor =
        TableDescriptor.forConnector("IoTDB").schema(dataGenTableSchema).build();
    tableEnv.createTemporaryTable("iotdbSinkTable", iotdbDescriptor);

    // insert data
    dataGenTable.executeInsert("iotdbSinkTable").print();
  }
}
