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
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class InsertHandler extends Handler{
  public JSON handle(Object json)
      throws IllegalPathException, QueryProcessException,
      StorageEngineException, StorageGroupNotSetException, AuthException {
    checkLogin();
    JSONArray array = (JSONArray) json;
    for (Object o : array) {
      JSONObject object = (JSONObject) o;
      String deviceID = (String) object.get(HttpConstant.DEVICE_ID);
      JSONArray measurements = (JSONArray) object.get(HttpConstant.MEASUREMENTS);
      long timestamps = (Integer) object.get(HttpConstant.TIMESTAMP);
      JSONArray values  = (JSONArray) object.get(HttpConstant.VALUES);
      Boolean isNeedInferType = (Boolean) object.get(HttpConstant.IS_NEED_INFER_TYPE);
      if (!insertByRow(deviceID, timestamps, getListString(measurements), values, isNeedInferType)) {
          throw new QueryProcessException(
              String.format("%s can't be inserted successfully", deviceID));
        }
    }
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(HttpConstant.RESULT, HttpConstant.SUCCESSFUL_OPERATION);
    return jsonObject;
  }

  private boolean insertByRow(String deviceId, long time, List<String> measurements,
      List<Object> values, boolean isNeedInferType)
      throws IllegalPathException, QueryProcessException, StorageEngineException, StorageGroupNotSetException {
    InsertRowPlan plan = new InsertRowPlan();
    plan.setDeviceId(new PartialPath(deviceId));
    plan.setTime(time);
    plan.setMeasurements(measurements.toArray(new String[0]));
    plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
    if(isNeedInferType) {
      plan.setNeedInferType(true);
    } else {
      TSDataType[] dataTypes = new TSDataType[measurements.size()];
      for(int i = 0; i < measurements.size(); i++) {
        if(values.get(i) instanceof Integer) {
          dataTypes[i] = TSDataType.INT32;
        } else if(values.get(i) instanceof String) {
          dataTypes[i] = TSDataType.TEXT;
        } else if(values.get(i) instanceof Float) {
          dataTypes[i] = TSDataType.FLOAT;
        } else if(values.get(i) instanceof Boolean) {
          dataTypes[i] = TSDataType.BOOLEAN;
        } else {
          throw new QueryProcessException("Unsupported json data type:" + dataTypes[i]);
        }
      }
      plan.setDataTypes(dataTypes);
      plan.setNeedInferType(false);
    }
    plan.setValues(values.toArray(new Object[0]));
    return executor.processNonQuery(plan);
  }

  /**
   * transform JsonArray to List<String>
   */
  private List<String> getListString(JSONArray jsonArray) {
    List<String> list = new ArrayList<>();
    for (Object o : jsonArray) {
      list.add((String) o);
    }
    return list;
  }


}
