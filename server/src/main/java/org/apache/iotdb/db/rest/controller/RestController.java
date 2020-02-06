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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.rest.model.TimeValues;
import org.apache.iotdb.db.rest.service.RestService;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It’s used for mapping http request.
 */

@Path("/")
public class RestController {

  private static final Logger logger = LoggerFactory.getLogger(RestController.class);
  private RestService restService = new RestService();

  /**
   * http request to login IoTDB
   * @param username username for login IoTDB
   * @param password password for login IoTDB
   */

  @Path("/login/{username}&&{password}")
  @POST
  public void login(@PathParam("username") String username, @PathParam("password") String password)
      throws AuthException {
    logger.info("{}: receive http request from username {}", IoTDBConstant.GLOBAL_DB_NAME,
        username);
    IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
    boolean status = authorizer.login(username, password);
    if (status) {
      restService.setUsername(username);
      logger.info("{}: Login successfully. User : {}", IoTDBConstant.GLOBAL_DB_NAME, username);
    } else {
      throw new AuthException("Wrong login password");
    }
  }

  /**
   *
   * @param request this request will be in json format.
   * @param response this response will be in json format.
   * @return json in String
   */
  @Path("/query")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public String query(HttpServletRequest request, HttpServletResponse response) {
    String targetStr = "target";
    response.setStatus(200);
    try {
      JSONObject jsonObject = getRequestBodyJson(request);
      assert jsonObject != null;
      JSONObject range = (JSONObject) jsonObject.get("range");
      Pair<String, String> timeRange = new Pair<>((String) range.get("from"), (String) range.get("to"));
      JSONArray array = (JSONArray) jsonObject.get("targets"); // []
      JSONArray result = new JSONArray();
      for (int i = 0; i < array.size(); i++) {
        JSONObject object = (JSONObject) array.get(i); // {}
        if (!object.containsKey(targetStr)) {
          return "[]";
        }
        String target = (String) object.get(targetStr);
        String type = getJsonType(jsonObject);
        JSONObject obj = new JSONObject();
        obj.put("target", target);
        if (type.equals("table")) {
          setJsonTable(obj, target, timeRange);
        } else if (type.equals("timeserie")) {
          setJsonTimeseries(obj, target, timeRange);
        }
        result.add(i, obj);
      }
      logger.info("query finished");
      return result.toString();
    } catch (Exception e) {
      logger.error("/query failed", e);
    }
    return null;
  }

  /**
   * get request body JSON.
   *
   * @param request http request
   * @return request JSON
   * @throws JSONException JSONException
   */
  private JSONObject getRequestBodyJson(HttpServletRequest request) throws JSONException {
    try {
      BufferedReader br = request.getReader();
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
   * get JSON type of input JSON object.
   *
   * @param jsonObject JSON Object
   * @return type (string)
   * @throws JSONException JSONException
   */
  private String getJsonType(JSONObject jsonObject) throws JSONException {
    JSONArray array = (JSONArray) jsonObject.get("targets"); // []
    JSONObject object = (JSONObject) array.get(0); // {}
    return (String) object.get("type");
  }

  private void setJsonTable(JSONObject obj, String target,
      Pair<String, String> timeRange)
      throws JSONException, StorageEngineException, QueryFilterOptimizationException,
      MetadataException, IOException, StorageGroupException, SQLException, QueryProcessException, AuthException {
    List<TimeValues> timeValues = restService.querySeries(target, timeRange);
    JSONArray columns = new JSONArray();
    JSONObject column = new JSONObject();
    column.put("text", "Time");
    column.put("type", "time");
    columns.add(column);
    column = new JSONObject();
    column.put("text", "Number");
    column.put("type", "number");
    columns.add(column);
    obj.put("columns", columns);
    JSONArray values = new JSONArray();
    for (TimeValues tv : timeValues) {
      JSONArray value = new JSONArray();
      value.add(tv.getTime());
      value.add(tv.getValue());
      values.add(value);
    }
    obj.put("values", values);
  }

  private void setJsonTimeseries(JSONObject obj, String target,
      Pair<String, String> timeRange)
      throws JSONException, StorageEngineException, QueryFilterOptimizationException,
      MetadataException, IOException, StorageGroupException, SQLException, QueryProcessException, AuthException {
    List<TimeValues> timeValues = restService.querySeries(target, timeRange);
    logger.info("query size: {}", timeValues.size());
    JSONArray dataPoints = new JSONArray();
    for (TimeValues tv : timeValues) {
      long time = tv.getTime();
      String value = tv.getValue();
      JSONArray jsonArray = new JSONArray();
      jsonArray.add(value);
      jsonArray.add(time);
      dataPoints.add(jsonArray);
    }
    obj.put("datapoints", dataPoints);
  }

}
