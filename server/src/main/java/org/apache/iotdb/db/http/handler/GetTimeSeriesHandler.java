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
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;

public class GetTimeSeriesHandler extends Handler {
  public JSON handle(Object json)
      throws AuthException, MetadataException, TException, StorageEngineException,
      QueryFilterOptimizationException, IOException, InterruptedException, SQLException, QueryProcessException {
    checkLogin();
    JSONArray jsonArray = (JSONArray) json;
    JSONArray result = new JSONArray();
    for(Object object : jsonArray) {
      String path = (String) object;
      PartialPath partialPath = new PartialPath(path);
      ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(partialPath);
      plan.setHasLimit(true);
      if(!AuthorityChecker.check(username, plan.getPaths(), plan.getOperatorType(), null)) {
        throw new AuthException(String.format("%s can't be gotten by %s", path, username));
      }
      long queryID = QueryResourceManager.getInstance().assignQueryId(false);
      QueryDataSet dataSet = executor.processQuery(plan, new QueryContext(queryID));
      while(dataSet.hasNext()) {
        JSONArray row = new JSONArray();
        RowRecord rowRecord = dataSet.next();
        List<Field> fields = rowRecord.getFields();
        for(Field field : fields) {
          if(field != null) {
            row.add(field.getStringValue());
          } else {
            row.add(HttpConstant.NULL);
          }
        }
        result.add(row);
      }
    }
    return result;
  }
}
