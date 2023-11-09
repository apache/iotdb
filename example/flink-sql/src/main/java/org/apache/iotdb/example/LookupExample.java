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
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

public class LookupExample {
  public static void main(String[] args) {
    // setup environment
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    // register left table
    Schema dataGenTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("s0", DataTypes.INT())
            .build();

    TableDescriptor datagenDescriptor =
        TableDescriptor.forConnector("datagen")
            .schema(dataGenTableSchema)
            .option("fields.Time_.kind", "sequence")
            .option("fields.Time_.start", "1")
            .option("fields.Time_.end", "5")
            .option("fields.s0.min", "1")
            .option("fields.s0.max", "1")
            .build();
    tableEnv.createTemporaryTable("leftTable", datagenDescriptor);

    // register right table
    Schema iotdbTableSchema =
        Schema.newBuilder()
            .column("Time_", DataTypes.BIGINT())
            .column("root.sg.d0.s0", DataTypes.FLOAT())
            .column("root.sg.d1.s0", DataTypes.FLOAT())
            .column("root.sg.d1.s1", DataTypes.FLOAT())
            .build();

    TableDescriptor iotdbDescriptor =
        TableDescriptor.forConnector("IoTDB")
            .schema(iotdbTableSchema)
            .option("sql", "select ** from root")
            .build();
    tableEnv.createTemporaryTable("rightTable", iotdbDescriptor);

    // join
    String sql =
        "SELECT l.Time_, l.s0,r.`root.sg.d0.s0`, r.`root.sg.d1.s0`, r.`root.sg.d1.s1`"
            + "FROM (select *,PROCTIME() as proc_time from leftTable) AS l "
            + "JOIN rightTable FOR SYSTEM_TIME AS OF l.proc_time AS r "
            + "ON l.Time_ = r.Time_";

    // output table
    tableEnv.sqlQuery(sql).execute().print();
  }
}
