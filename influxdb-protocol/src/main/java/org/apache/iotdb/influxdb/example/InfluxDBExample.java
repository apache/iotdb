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

package org.apache.iotdb.influxdb.example;

import org.apache.iotdb.influxdb.IoTDBInfluxDBFactory;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBExample {

  private static InfluxDB influxDB;

  public static void main(String[] args) throws Exception {
    influxDB = IoTDBInfluxDBFactory.connect("http://127.0.0.1:6667", "root", "root");
    influxDB.createDatabase("database");
    influxDB.setDatabase("database");
    insertData();
    queryData();
  }

  private static void insertData() {
    Point.Builder builder = Point.measurement("student");
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    tags.put("name", "xie");
    tags.put("sex", "m");
    fields.put("score", 87.0);
    fields.put("tel", "110");
    fields.put("country", "china");
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Point point = builder.build();
    influxDB.write(point);

    builder = Point.measurement("student");
    tags = new HashMap<>();
    fields = new HashMap<>();
    tags.put("name", "xie");
    tags.put("sex", "m");
    tags.put("province", "anhui");
    fields.put("score", 99);
    fields.put("country", "china");
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    point = builder.build();
    influxDB.write(point);
  }

  private static void queryData() {
    Query query;
    QueryResult result;

    // the selector query is parallel to the field value
    query =
        new Query(
            "select * from student where (name=\"xie\" and sex=\"m\")or time<now()-7d", "database");
    result = influxDB.query(query);
    System.out.println("query1 result:" + result.getResults().get(0).getSeries().get(0).toString());

    // use iotdb built-in func
    query =
        new Query(
            "select max(score),min(score),sum(score),count(score),spread(score),mean(score),first(score),last(score) from student ",
            "database");
    result = influxDB.query(query);
    System.out.println("query2 result:" + result.getResults().get(0).getSeries().get(0).toString());

    // aggregate query and selector query are parallel
    query =
        new Query(
            "select count(score),first(score),last(country),max(score),mean(score),median(score),min(score),mode(score),spread(score),stddev(score),sum(score) from student where (name=\"xie\" and sex=\"m\")or score<99",
            "database");
    result = influxDB.query(query);
    System.out.println("query3 result:" + result.getResults().get(0).getSeries().get(0).toString());
  }
}
