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
package org.apache.iotdb.db.rest.controller;

import static org.apache.iotdb.db.rest.constant.RestConstant.COLON;
import static org.apache.iotdb.db.rest.constant.RestConstant.COMPRESSOR;
import static org.apache.iotdb.db.rest.constant.RestConstant.CREATE_TIME_SERIES;
import static org.apache.iotdb.db.rest.constant.RestConstant.DATA_TYPE;
import static org.apache.iotdb.db.rest.constant.RestConstant.DEVICE_ID;
import static org.apache.iotdb.db.rest.constant.RestConstant.ENCODING;
import static org.apache.iotdb.db.rest.constant.RestConstant.FAIL;
import static org.apache.iotdb.db.rest.constant.RestConstant.INSERT;
import static org.apache.iotdb.db.rest.constant.RestConstant.MEASUREMENTS;
import static org.apache.iotdb.db.rest.constant.RestConstant.NO_TARGET;
import static org.apache.iotdb.db.rest.constant.RestConstant.REQUEST_NULL;
import static org.apache.iotdb.db.rest.constant.RestConstant.SET_STORAGE_GROUP;
import static org.apache.iotdb.db.rest.constant.RestConstant.SUCCESS;
import static org.apache.iotdb.db.rest.constant.RestConstant.TARGET;
import static org.apache.iotdb.db.rest.constant.RestConstant.TARGETS;
import static org.apache.iotdb.db.rest.constant.RestConstant.TIME;
import static org.apache.iotdb.db.rest.constant.RestConstant.TYPE;
import static org.apache.iotdb.db.rest.constant.RestConstant.VALUES;
import static org.apache.iotdb.db.rest.constant.RestConstant.WRONG_TYPE;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.security.PermitAll;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metrics.MetricsSystem;
import org.apache.iotdb.db.rest.service.RestService;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It’s used for mapping http request.
 */

@Path("/")
public class RestController {

  private static final Logger logger = LoggerFactory.getLogger(RestController.class);
  private RestService restService = RestService.getInstance();
  private MetricsSystem metricsSystem = new MetricsSystem();

  /**
   *
   * @param request this request will be in metricsJson format.
   * @return metricsJson in String
   */
  @Path("/query")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public JSONArray query(@Context HttpServletRequest request) {
    JSONObject jsonObject = getRequestBodyJson(request);
    if(jsonObject == null) {
      JSONArray jsonArray = new JSONArray();
      jsonArray.add(REQUEST_NULL);
      return jsonArray;
    }
    JSONObject range = (JSONObject) jsonObject.get("range");
    Pair<String, String> timeRange = new Pair<>((String) range.get("from"), (String) range.get("to"));
    JSONArray array = (JSONArray) jsonObject.get(TARGETS); // metricsJson array is []
    JSONArray result = new JSONArray();
    for (int i = 0; i < array.size(); i++) {
      JSONObject object = (JSONObject) array.get(i);
      if (!object.containsKey(TARGET)) {
        result.add(JSON.parse("[]"));
        return result;
      }
      String timeseries = (String) object.get(TARGET);
      String type = restService.getJsonType(jsonObject);
      JSONObject obj = new JSONObject();
      obj.put(TARGET, timeseries);
      if (type.equals("table")) {
        try {
          restService.setJsonTable(obj, timeseries, timeRange);
        } catch (Exception e) {
          result.add(i, timeseries + COLON + e.getMessage());
        }
      } else if (type.equals("timeserie")) {
        try {
          restService.setJsonTimeseries(obj, timeseries, timeRange);
        } catch (Exception e) {
          result.add(i, timeseries + COLON + e.getMessage());
        }
      }
      result.add(i, obj);
    }
    return result;
  }

  @Path("/insert")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public String insert(@Context HttpServletRequest request) {
    JSONObject jsonObject = getRequestBodyJson(request);
    if(jsonObject == null) {
      return REQUEST_NULL;
    }
    String type = (String) jsonObject.get(TYPE);
    if(!type.equals(INSERT)) {
      return WRONG_TYPE;
    }
    JSONArray array = (JSONArray) jsonObject.get(TARGETS);
    JSONArray result = new JSONArray();
    for (Object o : array) {
      JSONObject object = (JSONObject) o;
      if (!object.containsKey(TARGET)) {
        result.add(NO_TARGET);
      }
      String deviceID = (String) object.get(DEVICE_ID);
      JSONArray measurements = (JSONArray) object.get(MEASUREMENTS);
      String time = (String) object.get(TIME);
      JSONArray values  = (JSONArray) object.get(VALUES);
      try {
        if (restService.insert(deviceID, Long.parseLong(time), getList(measurements), getList(values))) {
          result.add(deviceID + COLON + SUCCESS);
        } else {
          result.add(deviceID + COLON + FAIL);
        }
      } catch (QueryProcessException e) {
        result.add(deviceID + COLON + e.getMessage());
      }
    }
    return result.toString();
  }

  @Path("/createTimeSeries")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public String createTimeSeries(@Context HttpServletRequest request) {
    JSONObject jsonObject = getRequestBodyJson(request);
    if(jsonObject == null) {
      return REQUEST_NULL;
    }
    String type = (String) jsonObject.get(TYPE);
    if(!type.equals(CREATE_TIME_SERIES)) {
      return WRONG_TYPE;
    }
    JSONArray array = (JSONArray) jsonObject.get(TARGETS);
    JSONArray result = new JSONArray();
    for (Object o : array) {
      JSONObject object = (JSONObject) o;
      if (!object.containsKey(TARGET)) {
        result.add(NO_TARGET);
      }
      String timeseries = (String) object.get(TARGET);
      String dataType = (String) object.get(DATA_TYPE);
      String encoding = (String) object.get(ENCODING);
      String compressor = (String) object.get(COMPRESSOR);
      try {
        if (restService.createTimeSeries(timeseries, dataType, encoding, compressor)) {
          result.add(timeseries + COLON + SUCCESS);
        } else {
          result.add(timeseries + COLON + FAIL);
        }
      } catch (QueryProcessException e) {
        result.add(timeseries + COLON + e.getMessage());
      }
    }
    return result.toString();
  }

  @Path("/setStorageGroup")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public String setStorageGroup(@Context HttpServletRequest request) {
    JSONObject jsonObject = getRequestBodyJson(request);
    if(jsonObject == null) {
      return REQUEST_NULL;
    }
    String type = (String) jsonObject.get(TYPE);
    if(!type.equals(SET_STORAGE_GROUP)) {
      return WRONG_TYPE;
    }
    JSONArray array = (JSONArray) jsonObject.get(TARGETS);
    JSONArray result = new JSONArray();
    for (Object o : array) {
      String timeseries = (String) o;
      try {
        if (restService.setStorageGroup(timeseries)) {
          result.add(timeseries + COLON + SUCCESS);
        } else {
          result.add(timeseries + COLON + FAIL);
        }
      } catch (QueryProcessException e) {
        result.add(timeseries + COLON + e.getMessage());
      }
    }
    return result.toString();
  }

  /**
   * transform JsonArray to List<String>
   */
  private List<String> getList(JSONArray jsonArray) {
    List<String> list = new ArrayList<>();
    for (int i = 0; i <= jsonArray.size(); i++) {
      list.add((String) jsonArray.get(i));
    }
    return list;
  }

  /**
   * get request body JSON.
   *
   * @param request http request
   * @return request JSON
   * @throws JSONException JSONException
   */
  private JSONObject getRequestBodyJson(HttpServletRequest request){
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(request.getInputStream()));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      return JSON.parseObject(sb.toString());
    } catch (IOException e) {
      logger.error("getRequestBodyJson failed", e);
    }
    return null;
  }

  /**
   * get metrics in json format
   */
  @PermitAll
  @Path("/metrics_information")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getMetricsInformation() throws JsonProcessingException {
    return metricsSystem.metricsJson();
  }

  @PermitAll
  @Path("/server_information")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getServerInformation() {
    return metricsSystem.serverJson();
  }

  /**
   * get sql argument
   */
  @PermitAll
  @Path("/sql_arguments")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public JSONArray getSqlArguments() {
    return metricsSystem.sqlJson();
  }

  @PermitAll
  @Path("/version")
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getVersion() {
    return IoTDBConstant.VERSION;
  }
}
