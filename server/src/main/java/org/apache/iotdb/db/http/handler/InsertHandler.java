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
  public JSON handle(JSON json)
      throws IllegalPathException, QueryProcessException,
      StorageEngineException, StorageGroupNotSetException, AuthException {
    checkLogin();
    JSONArray array = (JSONArray) json;
    for (Object o : array) {
      JSONObject object = (JSONObject) o;
      String deviceID = (String) object.get(HttpConstant.DEVICE_ID);
      JSONArray measurements = (JSONArray) object.get(HttpConstant.MEASUREMENTS);
      JSONArray timestamps = (JSONArray) object.get(HttpConstant.TIMESTAMPS);
      JSONArray values  = (JSONArray) object.get(HttpConstant.VALUES);
      for(int i = 0; i < timestamps.size(); i++){
        if (!insert(deviceID, (Integer) timestamps.get(i), getListString(measurements), (JSONArray)values.get(i))) {
          throw new QueryProcessException(String.format("%s can't be inserted successfully", deviceID));
        }
      }
    }
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(HttpConstant.RESULT, HttpConstant.SUCCESSFUL_OPERATION);
    return jsonObject;
  }

  public boolean insert(String deviceId, long time, List<String> measurements,
      List<Object> values)
      throws IllegalPathException, QueryProcessException, StorageEngineException, StorageGroupNotSetException {
    InsertRowPlan plan = new InsertRowPlan();
    plan.setDeviceId(new PartialPath(deviceId));
    plan.setTime(time);
    plan.setMeasurements(measurements.toArray(new String[0]));
    plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
    plan.setNeedInferType(true);
    plan.setValues(values.toArray(new Object[0]));
    return  executor.processNonQuery(plan);
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
