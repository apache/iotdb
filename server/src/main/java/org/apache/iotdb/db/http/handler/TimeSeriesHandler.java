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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.netty.handler.codec.http.HttpMethod;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.UnsupportedHttpMethod;
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
  public JSON handle(HttpMethod httpMethod, Object json)
      throws AuthException, MetadataException, QueryProcessException,
      StorageEngineException, UnsupportedHttpMethod {
    checkLogin();
    if (HttpMethod.POST.equals(httpMethod)) {
      JSONArray jsonArray = (JSONArray) json;
      for(Object object : jsonArray) {
        JSONObject jsonObject = (JSONObject) object;
        String path = (String) jsonObject.get(HttpConstant.TIME_SERIES);
        String dataType = (String) jsonObject.get(HttpConstant.DATATYPE);
        String encoding = (String) jsonObject.get(HttpConstant.ENCODING);
        String alias = (String) jsonObject.get(HttpConstant.ALIAS);
        String compression = (String) jsonObject.get(HttpConstant.COMPRESSION);
        JSONArray properties = (JSONArray) jsonObject.get(HttpConstant.PROPERTIES);
        JSONArray tags = (JSONArray) jsonObject.get(HttpConstant.TAGS);
        JSONArray attributes = (JSONArray) jsonObject.get(HttpConstant.ATTRIBUTES);
        CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan(new PartialPath(path),
            TSDataType.valueOf(dataType.toUpperCase()), TSEncoding.valueOf(encoding.toUpperCase()),
            compression == null? TSFileDescriptor.getInstance().getConfig().getCompressor() : CompressionType.valueOf(compression.toUpperCase()),
            jsonArrayToMap(properties), jsonArrayToMap(tags), jsonArrayToMap(attributes), alias);
        if(!AuthorityChecker.check(username, plan.getPaths(), plan.getOperatorType(), null)) {
          throw new AuthException(String.format("%s can't be created by %s", path, username));
        }
        if(!executor.processNonQuery(plan)) {
          throw new QueryProcessException(String.format("%s can't be created successfully", path));
        }
      }
      JSONObject result = new JSONObject();
      result.put(HttpConstant.RESULT, HttpConstant.SUCCESSFUL_OPERATION);
      return result;
    }  else {
      throw new UnsupportedHttpMethod(httpMethod.toString());
    }
  }

  private Map<String, String> jsonArrayToMap(JSONArray array) {
    if(array == null) {
      return null;
    }
    Map<String, String> map = new HashMap<>();
    for(Object object : array) {
      JSONObject json = (JSONObject) object;
      map.put((String)json.get(HttpConstant.KEY), (String)json.get(HttpConstant.VALUE));
    }
    return map;
  }
}
