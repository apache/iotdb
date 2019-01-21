/**
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

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.web.grafana.bean.TimeValues;
import org.apache.iotdb.web.grafana.service.DatabaseConnectService;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@CrossOrigin
@Controller
public class DatabaseConnectController {

  private static final Logger logger = LoggerFactory.getLogger(DatabaseConnectController.class);

  @Autowired
  private DatabaseConnectService databaseConnectService;

  @RequestMapping(value = "/", method = RequestMethod.GET)
  @ResponseStatus(value = HttpStatus.OK)
  public void testDataConnection(HttpServletResponse response) throws IOException {
    logger.info("Connection is ok now!");
    response.getWriter().print("I have sent a message.");
  }

  /**
   * get metrics numbers in JSON string structure.
   *
   * @param request http request
   * @param response http response
   * @return metrics numbers in JSON string structure
   */
  @RequestMapping(value = "/search")
  @ResponseBody
  public String metricFindQuery(HttpServletRequest request, HttpServletResponse response) {
    Map<Integer, String> target = new HashMap<>();
    response.setStatus(200);
    List<String> columnsName = new ArrayList<>();
    try {
      columnsName = databaseConnectService.getMetaData();
    } catch (Exception e) {
      logger.error("Failed to get metadata", e);
    }
    Collections.sort(columnsName);
    int cnt = 0;
    for (String columnName : columnsName) {
      target.put(cnt++, columnName);
    }
    JSONObject ojb = new JSONObject(target);
    return ojb.toString();
  }

  /**
   * convert query result data to JSON format.
   *
   * @param request http request
   * @param response http response
   * @return data in JSON format
   */
  @RequestMapping(value = "/query")
  @ResponseBody
  public String query(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(200);
    try {
      JSONObject jsonObject = getRequestBodyJson(request);
      Pair<ZonedDateTime, ZonedDateTime> timeRange = getTimeFromAndTo(jsonObject);
      JSONArray array = (JSONArray) jsonObject.get("targets"); // []
      JSONArray result = new JSONArray();
      for (int i = 0; i < array.length(); i++) {
        JSONObject object = (JSONObject) array.get(i); // {}
        if (object.isNull("target")) {
          return "[]";
        }
        String target = (String) object.get("target");
        String type = getJsonType(jsonObject);
        JSONObject obj = new JSONObject();
        obj.put("target", target);
        if (type.equals("table")) {
          setJsonTable(obj, target, timeRange);
        } else if (type.equals("timeserie")) {
          setJsonTimeseries(obj, target, timeRange);
        }
        result.put(i, obj);
      }
      logger.info("query finished");
      return result.toString();
    } catch (Exception e) {
      logger.error("/query failed", e);
    }
    return null;
  }

  private Pair<ZonedDateTime, ZonedDateTime> getTimeFromAndTo(JSONObject jsonObject)
      throws JSONException {
    JSONObject obj = (JSONObject) jsonObject.get("range");
    Instant from = Instant.parse((String) obj.get("from"));
    Instant to = Instant.parse((String) obj.get("to"));
    return new Pair<>(from.atZone(ZoneId.of("Asia/Shanghai")),
        to.atZone(ZoneId.of("Asia/Shanghai")));
  }

  private void setJsonTable(JSONObject obj, String target,
      Pair<ZonedDateTime, ZonedDateTime> timeRange)
      throws JSONException {
    List<TimeValues> timeValues = databaseConnectService.querySeries(target, timeRange);
    JSONArray columns = new JSONArray();
    JSONObject column = new JSONObject();
    column.put("text", "Time");
    column.put("type", "time");
    columns.put(column);
    column = new JSONObject();
    column.put("text", "Number");
    column.put("type", "number");
    columns.put(column);
    obj.put("columns", columns);
    JSONArray values = new JSONArray();
    for (TimeValues tv : timeValues) {
      JSONArray value = new JSONArray();
      value.put(tv.getTime());
      value.put(tv.getValue());
      values.put(value);
    }
    obj.put("values", values);
  }

  private void setJsonTimeseries(JSONObject obj, String target,
      Pair<ZonedDateTime, ZonedDateTime> timeRange)
      throws JSONException {
    List<TimeValues> timeValues = databaseConnectService.querySeries(target, timeRange);
    logger.info("query size: {}", timeValues.size());
    JSONArray dataPoints = new JSONArray();
    for (TimeValues tv : timeValues) {
      long time = tv.getTime();
      float value = tv.getValue();
      JSONArray jsonArray = new JSONArray();
      jsonArray.put(value);
      jsonArray.put(time);
      dataPoints.put(jsonArray);
    }
    obj.put("datapoints", dataPoints);
  }

  /**
   * get request body JSON.
   *
   * @param request http request
   * @return request JSON
   * @throws JSONException JSONException
   */
  public JSONObject getRequestBodyJson(HttpServletRequest request) throws JSONException {
    try {
      BufferedReader br = request.getReader();
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      return new JSONObject(sb.toString());
    } catch (IOException e) {
      logger.error("getRequestBodyJson failed", e);
    }
    return null;
  }

  /**
   * get JSON type of input JSON object.
   *
   * @param jsonObject JSON Object
   * @return type (string)
   * @throws JSONException JSONException
   */
  public String getJsonType(JSONObject jsonObject) throws JSONException {
    JSONArray array = (JSONArray) jsonObject.get("targets"); // []
    JSONObject object = (JSONObject) array.get(0); // {}
    return (String) object.get("type");
  }

}
