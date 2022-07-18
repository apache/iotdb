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
package org.apache.iotdb.web.grafana.controller;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.web.grafana.bean.TimeValues;
import org.apache.iotdb.web.grafana.service.DatabaseConnectService;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@CrossOrigin
@Controller
public class DatabaseConnectController {

  private static final Logger logger = LoggerFactory.getLogger(DatabaseConnectController.class);
  public static final Gson GSON = new GsonBuilder().create();

  @Autowired private DatabaseConnectService databaseConnectService;

  @RequestMapping(value = "/", method = RequestMethod.GET)
  @ResponseStatus(value = HttpStatus.OK)
  public void testDataConnection(HttpServletResponse response) throws IOException {
    logger.info("Connection is ok now!");
    response.getWriter().print("I have sent a message.");
  }

  /**
   * get metrics numbers in JSON string structure.
   *
   * @return metrics numbers in JSON string structure
   */
  @RequestMapping(value = "/search")
  @ResponseBody
  public String metricFindQuery() {
    JsonObject root = new JsonObject();
    List<String> columnsName = new ArrayList<>();
    try {
      columnsName = databaseConnectService.getMetaData();
    } catch (Exception e) {
      logger.error("Failed to get metadata", e);
    }
    Collections.sort(columnsName);
    for (int i = 0; i < columnsName.size(); i++) {
      root.addProperty(String.valueOf(i), columnsName.get(i));
    }
    return root.toString();
  }

  /**
   * query and return data in JSON format.
   *
   * @return data in JSON format
   */
  @RequestMapping(value = "/query", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
  @ResponseBody
  public String query(@RequestBody String json) {
    String targetStr = "target";
    try {
      JsonObject jsonObject = GSON.fromJson(json, JsonObject.class);
      if (Objects.isNull(jsonObject)) {
        return null;
      }
      Pair<ZonedDateTime, ZonedDateTime> timeRange = getTimeFromAndTo(jsonObject);
      JsonArray array = jsonObject.getAsJsonArray("targets");
      JsonArray result = new JsonArray();
      for (int i = 0; i < array.size(); i++) {
        JsonObject object = array.get(i).getAsJsonObject();
        if (!object.has(targetStr)) {
          continue;
        }
        String target = object.get(targetStr).getAsString();
        if (target.contains(";")) {
          throw new Exception("Only one SQL statement is supported");
        }
        JsonObject obj = new JsonObject();
        obj.addProperty("target", target);
        String type = getJsonType(object);
        if ("table".equals(type)) {
          setJsonTable(obj, target, timeRange);
        } else if ("timeserie".equals(type)) {
          setJsonTimeseries(obj, target, timeRange);
        }
        result.add(obj);
      }
      logger.info("query finished");
      return result.toString();
    } catch (Exception e) {
      logger.error("/query failed, request body is {}", json.replaceAll("[\n\r\t]", "_"), e);
    }
    return null;
  }

  private Pair<ZonedDateTime, ZonedDateTime> getTimeFromAndTo(JsonObject jsonObject) {
    JsonObject obj = jsonObject.getAsJsonObject("range");
    Instant from = Instant.parse(obj.get("from").getAsString());
    Instant to = Instant.parse(obj.get("to").getAsString());
    return new Pair<>(
        from.atZone(ZoneId.of("Asia/Shanghai")), to.atZone(ZoneId.of("Asia/Shanghai")));
  }

  private void setJsonTable(
      JsonObject obj, String target, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
    List<TimeValues> timeValues = databaseConnectService.querySeries(target, timeRange);
    JsonArray columns = new JsonArray();
    JsonObject column = new JsonObject();

    column.addProperty("text", "Time");
    column.addProperty("type", "time");
    columns.add(column);

    column = new JsonObject();
    column.addProperty("text", "Number");
    column.addProperty("type", "number");
    columns.add(column);

    obj.add("columns", columns);
    JsonArray values = new JsonArray();
    for (TimeValues tv : timeValues) {
      JsonArray value = new JsonArray();
      value.add(tv.getTime());
      value.add(GSON.toJsonTree(tv.getValue()));
      values.add(value);
    }

    obj.add("values", values);
  }

  private void setJsonTimeseries(
      JsonObject obj, String target, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
    List<TimeValues> timeValues = databaseConnectService.querySeries(target, timeRange);
    logger.info("query size: {}", timeValues.size());

    JsonArray dataPoints = new JsonArray();
    for (TimeValues tv : timeValues) {
      long time = tv.getTime();
      Object value = tv.getValue();
      JsonArray jsonArray = new JsonArray();
      jsonArray.add(GSON.toJsonTree(value));
      jsonArray.add(time);
      dataPoints.add(jsonArray);
    }
    obj.add("datapoints", dataPoints);
  }

  /**
   * get JSON type of input JSON object.
   *
   * @param jsonObject JSON Object
   * @return type (string)
   */
  public String getJsonType(JsonObject jsonObject) {
    return jsonObject.get("type").getAsString();
  }
}
