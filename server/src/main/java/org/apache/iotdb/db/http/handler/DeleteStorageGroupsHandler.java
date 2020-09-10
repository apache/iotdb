package org.apache.iotdb.db.http.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;

public class DeleteStorageGroupsHandler extends Handler{
  public JSON handle(Object json)
      throws IllegalPathException, AuthException,
      QueryProcessException, StorageEngineException, StorageGroupNotSetException {
    JSONArray jsonArray = (JSONArray) json;
    List<PartialPath> storageGroups = new ArrayList<>();
    for(Object object : jsonArray) {
      String storageGroup = (String) object;
      storageGroups.add(new PartialPath(storageGroup));
    }
    DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroups);
    if(!AuthorityChecker.check(username, plan.getPaths(), plan.getOperatorType(), null)) {
      throw new AuthException(String.format("%s can't be delete by %s", storageGroups, username));
    }
    if(!executor.processNonQuery(plan)) {
      throw new QueryProcessException(String.format("%s can't be deleted successfully", storageGroups));
    }
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(HttpConstant.RESULT, HttpConstant.SUCCESSFUL_OPERATION);
    return jsonObject;
  }
}
