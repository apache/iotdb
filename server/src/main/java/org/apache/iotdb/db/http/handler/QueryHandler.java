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
import java.util.*;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.crud.*;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;

public class QueryHandler extends Handler{

  public JSON handle(Object json)
      throws QueryProcessException, MetadataException, AuthException,
      TException, StorageEngineException, QueryFilterOptimizationException,
      IOException, InterruptedException, SQLException {
    checkLogin();
    JSONObject jsonObject = (JSONObject) json;
    Long from = (Long) jsonObject.get(HttpConstant.FROM);
    Long to = (Long) jsonObject.get(HttpConstant.TO);
    JSONArray timeSeries = (JSONArray) jsonObject.get(HttpConstant.TIME_SERIES);
    if(timeSeries == null) {
      JSONObject result = new JSONObject();
      result.put("result", "Has no timeSeries");
      return result;
    }
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    for(Object o : timeSeries) {
      fromOp.addPrefixTablePath(new PartialPath((String) o));
    }
    selectOp.addSelectPath(new PartialPath(new String[]{""}));
    Boolean isAggregated = (Boolean) jsonObject.get(HttpConstant.IS_AGGREGATED);
    QueryOperator queryOp = new QueryOperator(SQLConstant.TOK_QUERY);
    queryOp.setSelectOperator(selectOp);
    queryOp.setFromOperator(fromOp);

    //set time filter operator
    FilterOperator filterOp = new FilterOperator(SQLConstant.KW_AND);

    filterOp.setSinglePath(SQLConstant.TIME_PATH);
    Set<PartialPath> pathSet = new HashSet<>();
    pathSet.add(SQLConstant.TIME_PATH);
    filterOp.setIsSingle(true);
    filterOp.setPathSet(pathSet);
    BasicFunctionOperator left = new BasicFunctionOperator(SQLConstant.GREATERTHANOREQUALTO, SQLConstant.TIME_PATH, from.toString());
    BasicFunctionOperator right = new BasicFunctionOperator(SQLConstant.LESSTHAN, SQLConstant.TIME_PATH, to.toString());
    filterOp.addChildOperator(left);
    filterOp.addChildOperator(right);
    queryOp.setFilterOperator(filterOp);

    if(isAggregated) {
      selectOp.getSuffixPaths().clear();
      JSONArray fills = (JSONArray) jsonObject.get(HttpConstant.FILLS);
      Boolean isPoint = (Boolean) jsonObject.get(HttpConstant.isPoint);
      JSONObject groupBy = (JSONObject) jsonObject.get(HttpConstant.GROUP_BY);
      JSONArray aggregations = (JSONArray) jsonObject.get(HttpConstant.AGGREGATIONS);
      List<String> aggregationsList = new ArrayList<>();
      for(Object o: aggregations) {
        aggregationsList.add((String) o);
        selectOp.addSelectPath(new PartialPath(new String[]{""}));
      }
      selectOp.setAggregations(aggregationsList);
      String step = null;
      if (groupBy != null) {
        //set start and end time
        queryOp.setStartTime(from);
        queryOp.setEndTime(to);
        queryOp.setGroupByTime(true);

        // set unit and sliding step
        if (isPoint) {
          Integer points = (Integer) groupBy.get(HttpConstant.SAMPLING_POINTS);
          long unit = Math.abs(to - from) / points;
          queryOp.setUnit(unit);
          queryOp.setSlidingStep(unit);
        } else {
          String unit = (String) groupBy.get(HttpConstant.SAMPLING_INTERVAL);
          queryOp.setUnit(parseDuration(unit));
          step = (String) groupBy.get(HttpConstant.STEP);
          if (step != null && !step.equals("")) {
            queryOp.setSlidingStep(parseDuration(step));
          } else {
            queryOp.setSlidingStep(queryOp.getUnit());
          }
        }
        if(fills != null && (step == null || step.equals(""))) {
          queryOp.setFill(true);
          Map<TSDataType, IFill> fillTypes = new EnumMap<>(TSDataType.class);
          for (Object o : fills) {
            JSONObject fill = (JSONObject) o;
            long duration = parseDuration((String) fill.get(HttpConstant.DURATION));
            PreviousFill previousFill;
            if (fill.get(HttpConstant.PREVIOUS).equals(HttpConstant.PREVIOUS_UNTIL_LAST)) {
              previousFill = new PreviousFill(duration, true);
            } else {
              previousFill = new PreviousFill(duration);
            }
            fillTypes.put(TSDataType.valueOf((String) fill.get(HttpConstant.DATATYPE)), previousFill);
          }
          queryOp.setFillTypes(fillTypes);
        }
      } else {
        throw new QueryProcessException("Aggregation function must have group by");
      }
    }

    QueryPlan plan = (QueryPlan) processor.logicalPlanToPhysicalPlan(queryOp);
    if(!AuthorityChecker.check(username, plan.getPaths(), plan.getOperatorType(), null)) {
      throw new AuthException(String.format("%s can't be queried by %s", plan.getPaths(), username));
    }
    JSONArray result = new JSONArray();
    QueryDataSet dataSet = executor.processQuery(plan, new QueryContext(QueryResourceManager.getInstance().assignQueryId(true)));
    while(dataSet.hasNext()) {
      JSONObject datapoint = new JSONObject();
      RowRecord rowRecord = dataSet.next();
      for(Field field : rowRecord.getFields()) {
        datapoint.put(HttpConstant.TIMESTAMP, rowRecord.getTimestamp());
        datapoint.put(HttpConstant.VALUE,field.getObjectValue(field.getDataType()));
      }
      result.add(datapoint);
    }
    return result;
  }

  /**
   * parse duration to time value.
   *
   * @param durationStr represent duration string like: 12d8m9ns, 1y1mo, etc.
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  private Long parseDuration(String durationStr) {
    String timestampPrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();

    long total = 0;
    long tmp = 0;
    for (int i = 0; i < durationStr.length(); i++) {
      char ch = durationStr.charAt(i);
      if (Character.isDigit(ch)) {
        tmp *= 10;
        tmp += (ch - '0');
      } else {
        String unit = durationStr.charAt(i) + "";
        // This is to identify units with two letters.
        if (i + 1 < durationStr.length() && !Character.isDigit(durationStr.charAt(i + 1))) {
          i++;
          unit += durationStr.charAt(i);
        }
        total += DatetimeUtils
                .convertDurationStrToLong(tmp, unit.toLowerCase(), timestampPrecision);
        tmp = 0;
      }
    }
    if (total <= 0) {
      throw new SQLParserException("Interval must more than 0.");
    }
    return total;
  }
}
