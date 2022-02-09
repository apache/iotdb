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

package org.apache.iotdb.spark.db.tools;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.spark.db.IoTDBOptions;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.thrift.annotation.Nullable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.BOOLEAN;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.FLOAT;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT32;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT64;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

public class DataFrameTools {
  /**
   * * insert a narrow dataframe into IoTDB
   *
   * @param options the options to create a IoTDB session
   * @param dataFrame a dataframe of narrow table
   * @return
   */
  public static void insertDataFrame(IoTDBOptions options, Dataset<Row> dataFrame) {
    List<Tuple2<String, String>> sensorTypes = new ArrayList<>(Arrays.asList(dataFrame.dtypes()));
    sensorTypes.remove(0);
    sensorTypes.remove(0);

    List<Row> devices = dataFrame.select("device_name").distinct().collectAsList();

    Dataset<Row> repartition = dataFrame.repartition(dataFrame.col("device_name"));

    for (Row row : devices) {
      String device = row.get(0).toString();
      repartition
          .where(String.format("device_name == '%s'", device))
          .foreachPartition(
              partition -> {
                String[] hostPort = options.url().split("//")[1].replace("/", "").split(":");
                Session session =
                    new Session.Builder()
                        .host(hostPort[0])
                        .port(Integer.valueOf(hostPort[1]))
                        .username(String.valueOf(options.user()))
                        .password(String.valueOf(options.password()))
                        .build();
                session.open();
                partition.forEachRemaining(
                    record -> {
                      ArrayList<String> measurements = new ArrayList<>();
                      ArrayList<TSDataType> types = new ArrayList<>();
                      ArrayList<Object> values = new ArrayList<>();
                      for (int i = 2; i < record.length(); i++) {
                        Object value = record.get(i);
                        if (value == null) {
                          continue;
                        }
                        value =
                            typeTrans(record.get(i).toString(), getType(sensorTypes.get(i - 2)._2));

                        values.add(value);
                        measurements.add(sensorTypes.get(i - 2)._1);
                        types.add(getType(sensorTypes.get(i - 2)._2));
                      }
                      try {
                        session.insertRecord(
                            record.get(1).toString(),
                            (Long) record.get(0),
                            measurements,
                            types,
                            values);
                      } catch (IoTDBConnectionException e) {
                        e.printStackTrace();
                      } catch (StatementExecutionException e) {
                        e.printStackTrace();
                      }
                    });
                session.close();
              });
    }
  }
  /**
   * @param value
   * @param type
   * @return
   */
  private static Object typeTrans(String value, TSDataType type) {
    try {
      switch (type) {
        case TEXT:
          return value;
        case BOOLEAN:
          return Boolean.valueOf(value);
        case INT32:
          return Integer.valueOf(value);
        case INT64:
          return Long.valueOf(value);
        case FLOAT:
          return Float.valueOf(value);
        case DOUBLE:
          return Double.valueOf(value);
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * return the TSDataType
   *
   * @param typeStr
   * @return
   */
  private static TSDataType getType(String typeStr) {
    switch (typeStr) {
      case "StringType":
        return TEXT;
      case "BooleanType":
        return BOOLEAN;
      case "IntegerType":
        return INT32;
      case "LongType":
        return INT64;
      case "FloatType":
        return FLOAT;
      case "DoubleType":
        return DOUBLE;
      default:
        return null;
    }
  }
}
