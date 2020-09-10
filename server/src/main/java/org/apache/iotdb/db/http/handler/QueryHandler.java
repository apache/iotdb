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
import java.io.IOException;
import java.sql.SQLException;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.http.parser.QueryParser;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.thrift.TException;

public class QueryHandler extends Handler{
  public JSON handle(JSON json)
      throws QueryProcessException, MetadataException, AuthException,
      TException, StorageEngineException, QueryFilterOptimizationException,
      IOException, InterruptedException, SQLException {
    checkLogin();
    JSONObject jsonObject = (JSONObject) json;
    JSONObject range = (JSONObject) jsonObject.get(HttpConstant.RANGE);
    String prefixPath = (String) jsonObject.get(HttpConstant.FROM);
    String suffixPath = (String) jsonObject.get(HttpConstant.SELECT);
    Pair<String, String> timeRange = new Pair<>((String)range.get(HttpConstant.START), (String)range.get(HttpConstant.END));
    QueryOperator queryOperator = QueryParser.generateOperator(suffixPath, prefixPath, timeRange);
    QueryPlan plan = (QueryPlan) processor.logicalPlanToPhysicalPlan(queryOperator);
    if(!AuthorityChecker.check(username, plan.getPaths(), plan.getOperatorType(), null)) {
      throw new AuthException(String.format("%s can't be queried by %s", prefixPath + TsFileConstant.PATH_SEPARATOR
          + suffixPath, username));
    }
    JSONArray result = new JSONArray();
    QueryDataSet dataSet = executor.processQuery(plan, new QueryContext(QueryResourceManager.getInstance().assignQueryId(true)));
    while(dataSet.hasNext()) {
      JSONObject datapoint = new JSONObject();
      RowRecord rowRecord = dataSet.next();
      for(Field field : rowRecord.getFields()) {
        datapoint.put(HttpConstant.TIMESTAMPS, rowRecord.getTimestamp());
        datapoint.put(HttpConstant.VALUE,field.getObjectValue(field.getDataType()));
      }
      result.add(datapoint);
    }
    return result;
  }
}
