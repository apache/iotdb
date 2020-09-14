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
package org.apache.iotdb.db.http.router;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.UnsupportedHttpMethod;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.http.handler.DeleteStorageGroupsHandler;
import org.apache.iotdb.db.http.handler.DeleteTimeSeriesHandler;
import org.apache.iotdb.db.http.handler.GetTimeSeriesHandler;
import org.apache.iotdb.db.http.handler.InsertHandler;
import org.apache.iotdb.db.http.handler.QueryHandler;
import org.apache.iotdb.db.http.handler.StorageGroupsHandlers;
import org.apache.iotdb.db.http.handler.TimeSeriesHandler;
import org.apache.iotdb.db.http.handler.UsersHandler;
import org.apache.iotdb.db.http.utils.URIUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.thrift.TException;

/**
 * Router that contains information about both route matching and return json.
 */
public class Router {

  /**
   * matching url and return JSON
   * @param method http method
   * @param uri uri will be matched
   * @param json request JSON Object
   * @return JSON object, may be a JSONArray or JSONObject
   */
  public JSON route(HttpMethod method, String uri, Object json)
      throws AuthException, MetadataException, QueryProcessException
      , StorageEngineException, UnsupportedHttpMethod, SQLException, InterruptedException, QueryFilterOptimizationException, IOException, TException {
    QueryStringDecoder decoder = new QueryStringDecoder(uri);
    uri = URIUtils.removeParameter(uri);
    switch (uri) {
      case HttpConstant.ROUTING_STORAGE_GROUPS:
        StorageGroupsHandlers storageGroupsHandlers = new StorageGroupsHandlers();
        return storageGroupsHandlers.handle(method, json);
      case HttpConstant.ROUTING_TIME_SERIES:
        TimeSeriesHandler timeSeriesHandler = new TimeSeriesHandler();
        return timeSeriesHandler.handle(method, json);
      case HttpConstant.ROUTING_USER_LOGIN:
        if (UsersHandler.userLogin(decoder.parameters())) {
          JSONObject result = new JSONObject();
          result.put(HttpConstant.RESULT, HttpConstant.SUCCESSFUL_OPERATION);
          return result;
        } else {
          throw new AuthException(String.format("%s can't log in", UsersHandler.getUsername()));
        }
      case HttpConstant.ROUTING_USER_LOGOUT:
        if (UsersHandler.userLogout(decoder.parameters())) {
          JSONObject result = new JSONObject();
          result.put(HttpConstant.RESULT, HttpConstant.SUCCESSFUL_OPERATION);
          return result;
        } else {
          throw new AuthException(String.format("%s can't log out", UsersHandler.getUsername()));
        }
      case HttpConstant.ROUTING_QUERY:
        QueryHandler queryHandler = new QueryHandler();
        return queryHandler.handle(json);
      case HttpConstant.ROUTING_INSERT:
        InsertHandler insertHandler = new InsertHandler();
        return insertHandler.handle(json);
      case HttpConstant.ROUTING_STORAGE_GROUPS_DELETE:
        DeleteStorageGroupsHandler deleteStorageGroupsHandler = new DeleteStorageGroupsHandler();
        return deleteStorageGroupsHandler.handle(json);
      case HttpConstant.ROUTING_TIME_SERIES_DELETE:
        DeleteTimeSeriesHandler deleteTimeSeriesHandler = new DeleteTimeSeriesHandler();
        return deleteTimeSeriesHandler.handle(json);
      case HttpConstant.ROUTING_GET_TIME_SERIES:
        GetTimeSeriesHandler getTimeSeriesHandler = new GetTimeSeriesHandler();
        return getTimeSeriesHandler.handle(json);
      case "":
        JSONObject result = new JSONObject();
        result.put(HttpConstant.RESULT, "Hello, IoTDB");
        return result;
      default:
        throw new UnsupportedHttpMethod(String.format("%s can't be found" , uri));
    }
  }

}