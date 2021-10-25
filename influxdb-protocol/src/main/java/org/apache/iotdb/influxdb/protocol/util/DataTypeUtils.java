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

package org.apache.iotdb.influxdb.protocol.util;

import org.apache.iotdb.influxdb.protocol.dto.SessionPoint;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DataTypeUtils {

  /**
   * convert normal type to a type
   *
   * @param value need to convert value
   * @return corresponding TSDataType
   */
  public static TSDataType normalTypeToTSDataType(Object value) {
    if (value instanceof Boolean) {
      return TSDataType.BOOLEAN;
    } else if (value instanceof Integer) {
      return TSDataType.INT32;
    } else if (value instanceof Long) {
      return TSDataType.INT64;
    } else if (value instanceof Double) {
      return TSDataType.DOUBLE;
    } else if (value instanceof String) {
      return TSDataType.TEXT;
    } else {
      throw new InfluxDBException("Data type not valid: " + value.toString());
    }
  }

  public static SessionPoint sessionToSessionPoint(Session session) {
    EndPoint endPoint = null;
    String username = null;
    String password = null;

    // Get the property of session by reflection
    for (java.lang.reflect.Field reflectField : session.getClass().getDeclaredFields()) {
      reflectField.setAccessible(true);
      try {
        if (reflectField
                .getType()
                .getName()
                .equalsIgnoreCase("org.apache.iotdb.service.rpc.thrift.EndPoint")
            && reflectField.getName().equalsIgnoreCase("defaultEndPoint")) {
          endPoint = (EndPoint) reflectField.get(session);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.lang.String")
            && reflectField.getName().equalsIgnoreCase("username")) {
          username = (String) reflectField.get(session);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.lang.String")
            && reflectField.getName().equalsIgnoreCase("password")) {
          password = (String) reflectField.get(session);
        }
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    if (endPoint == null) {
      throw new InfluxDBException("session's ip and port is null");
    }
    return new SessionPoint(endPoint.ip, endPoint.port, username, password);
  }

  public static Collection<Point> recordsToPoints(String records, TimeUnit precision) {
    ArrayList<Point> points = new ArrayList<>();
    String[] recordsSplit = records.split("\n");
    for (String record : recordsSplit) {
      points.add(recordToPoint(record, precision));
    }
    return points;
  }

  private static Point recordToPoint(String record, TimeUnit precision) {
    Point.Builder builder;
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    try {
      int firstCommaIndex = record.indexOf(",");
      String measurement = record.substring(0, firstCommaIndex);
      String dataInfo = record.substring(firstCommaIndex + 1);
      String[] datas = dataInfo.split(" ");
      String tagString = datas[0];
      String fieldString = datas[1];
      String time = datas[2];
      builder = Point.measurement(measurement);
      // parser tags
      String[] tagsString = tagString.split(",");
      for (String tag : tagsString) {
        String[] tagKeyAndValue = tag.split("=");
        tags.put(tagKeyAndValue[0], tagKeyAndValue[1]);
      }
      // parser fields
      String[] fieldsString = fieldString.split(",");
      for (String field : fieldsString) {
        String[] fieldKeyAndValue = field.split("=");
        String fieldValue = fieldKeyAndValue[1];
        Object value = null;
        // string type
        if (fieldValue.charAt(0) == '\"' && fieldValue.charAt(fieldValue.length() - 1) == '\"') {
          value = fieldValue.substring(0, fieldValue.length() - 1);
        }
        // int type
        else if (field.charAt(fieldValue.length() - 1) == 'i') {
          value = Integer.valueOf(field.substring(0, field.length() - 1));
        } else {
          value = Double.valueOf(fieldValue);
        }
        fields.put(fieldKeyAndValue[0], value);
      }
      builder.time(Long.parseLong(time), precision);
    } catch (Exception e) {
      throw new InfluxDBException("record type is illegal,record is " + record);
    }
    builder.tag(tags);
    builder.fields(fields);
    return builder.build();
  }
}
