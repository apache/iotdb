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
import static org.apache.iotdb.db.rest.constant.RestConstant.FROM;
import static org.apache.iotdb.db.rest.constant.RestConstant.INSERT;
import static org.apache.iotdb.db.rest.constant.RestConstant.MEASUREMENTS;
import static org.apache.iotdb.db.rest.constant.RestConstant.NO_TARGET;
import static org.apache.iotdb.db.rest.constant.RestConstant.QUERY;
import static org.apache.iotdb.db.rest.constant.RestConstant.RANGE;
import static org.apache.iotdb.db.rest.constant.RestConstant.REQUEST_BODY_JSON_FAILED;
import static org.apache.iotdb.db.rest.constant.RestConstant.SET_STORAGE_GROUP;
import static org.apache.iotdb.db.rest.constant.RestConstant.SQL;
import static org.apache.iotdb.db.rest.constant.RestConstant.SUCCESS;
import static org.apache.iotdb.db.rest.constant.RestConstant.TABLE;
import static org.apache.iotdb.db.rest.constant.RestConstant.TARGET;
import static org.apache.iotdb.db.rest.constant.RestConstant.TARGETS;
import static org.apache.iotdb.db.rest.constant.RestConstant.TIMESERIE;
import static org.apache.iotdb.db.rest.constant.RestConstant.TIMESTAMPS;
import static org.apache.iotdb.db.rest.constant.RestConstant.TO;
import static org.apache.iotdb.db.rest.constant.RestConstant.TYPE;
import static org.apache.iotdb.db.rest.constant.RestConstant.UNCOMPRESSED;
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
import javax.ws.rs.core.Response;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metrics.MetricsSystem;
import org.apache.iotdb.db.rest.service.RestService;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * It’s used for mapping http request.
 */

@Path("/")
public class RestController {

  private RestService restService = RestService.getInstance();
  private MetricsSystem metricsSystem = MetricsSystem.getInstance();

  /**
   *
   * @param request this request will be in metricsJson format.
   * @return metricsJson in String
   */
  @Path("/query")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response query(@Context HttpServletRequest request) {
    boolean hasError = false;
    JSONObject jsonObject;
    try {
      jsonObject = getRequestBodyJson(request);
    } catch (IOException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(REQUEST_BODY_JSON_FAILED).build();
    }
    String type = (String) jsonObject.get(TYPE);
    if(!type.equals(QUERY)) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(WRONG_TYPE).build();
    }
    JSONObject range = (JSONObject) jsonObject.get(RANGE);
    Pair<String, String> timeRange = new Pair<>((String) range.get(FROM), (String) range.get(TO));
    JSONArray array = (JSONArray) jsonObject.get(TARGETS); // metricsJson array is []
    JSONArray result = new JSONArray();
    for (int i = 0; i < array.size(); i++) {
      JSONObject object = (JSONObject) array.get(i);
      String timeseries = (String) object.get(TARGET);
      String showType = restService.getJsonType(jsonObject);
      JSONObject obj = new JSONObject();
      obj.put(TARGET, timeseries);
      if (showType.equals(TABLE)) {
        try {
          restService.setJsonTable(obj, timeseries, timeRange);
        } catch (Exception e) {
          result.add(i, timeseries + COLON + e.getMessage());
          hasError = true;
        }
      } else if (showType.equals(TIMESERIE)) {
        try {
          restService.setJsonTimeseries(obj, timeseries, timeRange);
        } catch (Exception e) {
          result.add(i, timeseries + COLON + e.getMessage());
          hasError = true;
        }
      }
      result.add(i, obj);
    }

    if(!hasError) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(result).build();
    } else {
      return Response.status(Response.Status.OK).entity(result).build();
    }
  }

  @Path("/insert")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response insert(@Context HttpServletRequest request) {
    boolean hasError = false;
    JSONObject jsonObject;
    try {
      jsonObject = getRequestBodyJson(request);
    } catch (IOException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(REQUEST_BODY_JSON_FAILED).build();
    }
    String type = (String) jsonObject.get(TYPE);
    if(!type.equals(INSERT)) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(WRONG_TYPE).build();
    }
    JSONArray array = (JSONArray) jsonObject.get(TARGETS);
    JSONArray result = new JSONArray();
    for (Object o : array) {
      JSONObject object = (JSONObject) o;
      String deviceID = (String) object.get(DEVICE_ID);
      JSONArray measurements = (JSONArray) object.get(MEASUREMENTS);
      JSONArray timestamps = (JSONArray) object.get(TIMESTAMPS);
      JSONArray values  = (JSONArray) object.get(VALUES);
      for(int i = 0; i < timestamps.size(); i++){
        try {
          if (restService.insert(deviceID, (Integer)timestamps.get(i), getList(measurements), getList((JSONArray) values.get(i)))) {
            result.add(deviceID + COLON + SUCCESS);
          } else {
            result.add(deviceID + COLON + FAIL);
            hasError = true;
          }
        } catch (QueryProcessException e) {
          result.add(deviceID + COLON + e.getMessage());
          hasError = true;
        }
      }
    }
    if(!hasError) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(result).build();
    } else {
      return Response.status(Response.Status.OK).entity(result).build();
    }
  }

  @Path("/createTimeSeries")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createTimeSeries(@Context HttpServletRequest request) {
    JSONObject jsonObject;
    try {
      jsonObject = getRequestBodyJson(request);
    } catch (IOException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(REQUEST_BODY_JSON_FAILED).build();
    }
    String type = (String) jsonObject.get(TYPE);
    if(!type.equals(CREATE_TIME_SERIES)) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(WRONG_TYPE).build();
    }
    boolean hasError = false;
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
      if(compressor == null) {
        compressor = UNCOMPRESSED;
      }
      try {
        if (restService.createTimeSeries(timeseries, dataType, encoding, compressor)) {
          result.add(timeseries + COLON + SUCCESS);
        } else {
          result.add(timeseries + COLON + FAIL);
          hasError = true;
        }
      } catch (QueryProcessException e) {
        result.add(timeseries + COLON + e.getMessage());
        hasError = true;
      }
    }
    if(!hasError) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(result).build();
    } else {
      return Response.status(Response.Status.OK).entity(result).build();
    }
  }

  @Path("/setStorageGroup")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setStorageGroup(@Context HttpServletRequest request) {
    JSONObject jsonObject;
    boolean hasError = false;
    try {
      jsonObject = getRequestBodyJson(request);
    } catch (IOException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(REQUEST_BODY_JSON_FAILED).build();
    }
    String type = (String) jsonObject.get(TYPE);
    if(!type.equals(SET_STORAGE_GROUP)) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(WRONG_TYPE).build();
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
          hasError = true;
        }
      } catch (QueryProcessException e) {
        result.add(timeseries + COLON + e.getMessage());
        hasError = true;
      }
    }
    if(!hasError) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(result).build();
    } else {
      return Response.status(Response.Status.OK).entity(result).build();
    }
  }

  @Path("/sql")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response excuteSQL(@Context HttpServletRequest request) {
    JSONArray jsonArray;
    JSONObject jsonObject;
    try {
      jsonObject = getRequestBodyJson(request);
    } catch (IOException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(REQUEST_BODY_JSON_FAILED).build();
    }
    String sql = (String)jsonObject.get(SQL);
    try {
      jsonArray = restService.executeStatement(sql, 100);
    } catch (Exception e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
    }
    return Response.status(Response.Status.OK).entity(jsonArray).build();
  }

  /**
   * transform JsonArray to List<String>
   */
  private List<String> getList(JSONArray jsonArray) {
    List<String> list = new ArrayList<>();
    for (Object o : jsonArray) {
      list.add((String) o);
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
  private JSONObject getRequestBodyJson(HttpServletRequest request) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(request.getInputStream()));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = br.readLine()) != null) {
      sb.append(line);
    }
    return JSON.parseObject(sb.toString());
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
