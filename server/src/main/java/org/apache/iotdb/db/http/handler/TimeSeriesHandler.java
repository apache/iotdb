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
package org.apache.iotdb.db.http.handler;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.netty.handler.codec.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.UnsupportedHttpMethodException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class TimeSeriesHandler extends Handler {

  public JsonElement handle(HttpMethod httpMethod, JsonElement json)
      throws AuthException, MetadataException, QueryProcessException,
      StorageEngineException, UnsupportedHttpMethodException {
    checkLogin();
    if (HttpMethod.POST.equals(httpMethod)) {
      JsonArray jsonArray = json.getAsJsonArray();
      for (JsonElement object : jsonArray) {
        JsonObject jsonObject = object.getAsJsonObject();
        String path = jsonObject.get(HttpConstant.TIME_SERIES).getAsString();
        String dataType = jsonObject.get(HttpConstant.DATATYPE).getAsString();
        String encoding = jsonObject.get(HttpConstant.ENCODING).getAsString();
        String alias = null;
        if (jsonObject.get(HttpConstant.ALIAS) != null) {
          alias = jsonObject.get(HttpConstant.ALIAS).getAsString();
        }
        CompressionType compression;
        if (jsonObject.get(HttpConstant.COMPRESSION) == null) {
          compression = TSFileDescriptor.getInstance().getConfig().getCompressor();
        } else {
          compression = CompressionType
              .valueOf(jsonObject.get(HttpConstant.COMPRESSION).getAsString().toUpperCase());
        }
        JsonArray properties = jsonObject.getAsJsonArray(HttpConstant.PROPERTIES);
        JsonArray tags = jsonObject.getAsJsonArray(HttpConstant.TAGS);
        JsonArray attributes = jsonObject.getAsJsonArray(HttpConstant.ATTRIBUTES);
        CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan(new PartialPath(path),
            TSDataType.valueOf(dataType.toUpperCase()), TSEncoding.valueOf(encoding.toUpperCase()),
            compression,
            jsonArrayToMap(properties), jsonArrayToMap(tags), jsonArrayToMap(attributes), alias);
        if (!AuthorityChecker.check(username, plan.getPaths(), plan.getOperatorType(), null)) {
          throw new AuthException(String.format("%s can't be created by %s", path, username));
        }
        if (!executor.processNonQuery(plan)) {
          throw new QueryProcessException(String.format("%s can't be created successfully", path));
        }
      }
      return getSuccessfulObject();
    } else {
      throw new UnsupportedHttpMethodException(httpMethod.toString());
    }
  }

  private Map<String, String> jsonArrayToMap(JsonArray array) {
    if (array == null) {
      return null;
    }
    Map<String, String> map = new HashMap<>();
    for (JsonElement object : array) {
      JsonObject json = object.getAsJsonObject();
      map.put(json.getAsJsonPrimitive(HttpConstant.KEY).getAsString(),
          json.getAsJsonPrimitive(HttpConstant.VALUE).getAsString());
    }
    return map;
  }
}
