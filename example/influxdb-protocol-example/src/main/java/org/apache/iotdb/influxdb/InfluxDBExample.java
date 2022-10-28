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

package org.apache.iotdb.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBExample {

  private static InfluxDB influxDB;

  private static final String database = "monitor";

  private static final String measurement = "factory";

  public static void main(String[] args) {
    influxDB = IoTDBInfluxDBFactory.connect("http://127.0.0.1:8086", "root", "root");
    influxDB.createDatabase(database);
    influxDB.setDatabase(database);
    insertData();
    queryData();
    influxDB.close();
  }

  private static void insertData() {
    Point.Builder builder = Point.measurement(measurement);
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    tags.put("workshop", "A1");
    tags.put("production", "B1");
    tags.put("cell", "C1");
    fields.put("temperature", 16.9);
    fields.put("pressure", 142);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Point point = builder.build();
    influxDB.write(point);

    builder = Point.measurement(measurement);
    tags = new HashMap<>();
    fields = new HashMap<>();
    tags.put("workshop", "A1");
    tags.put("production", "B1");
    tags.put("cell", "C2");
    fields.put("temperature", 16.5);
    fields.put("pressure", 108);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    point = builder.build();
    influxDB.write(point);

    builder = Point.measurement(measurement);
    tags = new HashMap<>();
    fields = new HashMap<>();
    tags.put("workshop", "A1");
    tags.put("production", "B2");
    tags.put("cell", "C2");
    fields.put("temperature", 13.0);
    fields.put("pressure", 130);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    point = builder.build();
    influxDB.write(point);
  }

  private static void queryData() {
    Query query;
    QueryResult result;

    query =
        new Query(
            "select * from factory where (workshop=\"A1\" and production=\"B1\" and cell =\"C1\" and time>now()-7d)",
            database);
    result = influxDB.query(query);
    System.out.println("query1 result:" + result.getResults().get(0).getSeries().get(0).toString());

    query =
        new Query(
            "select count(temperature),first(temperature),last(temperature),max(temperature),mean(temperature),median(temperature),min(temperature),mode(temperature),spread(temperature),stddev(temperature),sum(temperature) from factory where ((workshop=\"A1\" and production=\"B1\" and cell =\"C1\" ) or temperature< 15 )",
            database);
    result = influxDB.query(query);
    System.out.println("query2 result:" + result.getResults().get(0).getSeries().get(0).toString());
  }
}
