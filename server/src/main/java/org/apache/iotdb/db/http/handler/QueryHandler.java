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

import com.google.gson.*;

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

public class QueryHandler extends Handler {

  public JsonElement handle(JsonObject json)
      throws QueryProcessException, MetadataException, AuthException,
      TException, StorageEngineException, QueryFilterOptimizationException,
      IOException, InterruptedException, SQLException {
    checkLogin();
    long from;
    long to;
    from = json.get(HttpConstant.FROM).getAsLong();
    to = json.get(HttpConstant.TO).getAsLong();

    JsonArray timeSeries = json.getAsJsonArray(HttpConstant.TIME_SERIES);
    if (timeSeries == null) {
      JsonObject result = new JsonObject();
      result.addProperty("result", "Has no timeSeries");
      return result;
    }
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    String[] timeSeriesPath = new String[timeSeries.size()];
    for(int i = 0; i < timeSeries.size(); i++) {
      timeSeriesPath[i] = timeSeries.get(i).getAsString();
    }
    selectOp.addSelectPath(new PartialPath(timeSeriesPath));
    boolean isAggregated;
    fromOp.addPrefixTablePath(new PartialPath(new String[]{""}));
    if (json.get(HttpConstant.AGGREGATION) != null) {
      isAggregated = true;
      String aggregation = json.getAsJsonPrimitive(HttpConstant.AGGREGATION).getAsString();
      List<String> aggregationsList = new ArrayList<>();
      aggregationsList.add(aggregation);
      selectOp.setAggregations(aggregationsList);
    } else {
      isAggregated = false;
    }
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
    BasicFunctionOperator left = new BasicFunctionOperator(SQLConstant.GREATERTHANOREQUALTO,
        SQLConstant.TIME_PATH, Long.toString(from));
    BasicFunctionOperator right = new BasicFunctionOperator(SQLConstant.LESSTHAN,
        SQLConstant.TIME_PATH, Long.toString(to));
    filterOp.addChildOperator(left);
    filterOp.addChildOperator(right);
    queryOp.setFilterOperator(filterOp);

    if (isAggregated) {
      JsonArray fills = json.getAsJsonArray(HttpConstant.FILLS);
      JsonObject groupBy = json.getAsJsonObject(HttpConstant.GROUP_BY);
      String step = null;
      if (groupBy != null) {
        //set start and end time
        queryOp.setStartTime(from);
        queryOp.setEndTime(to);
        queryOp.setGroupByTime(true);

        JsonElement jsonUnit = groupBy.get(HttpConstant.SAMPLING_INTERVAL);
        String unit = "";
        if(jsonUnit != null) {
          unit = jsonUnit.getAsString();
        }
        // set unit and sliding step
        if (jsonUnit != null && !unit.equals("")) {
          unit = groupBy.get(HttpConstant.SAMPLING_INTERVAL).getAsString();
          queryOp.setUnit(parseDuration(unit));
          step = groupBy.get(HttpConstant.STEP).getAsString();
          if (step != null && !step.equals("")) {
            queryOp.setSlidingStep(parseDuration(step));
          } else {
            queryOp.setSlidingStep(queryOp.getUnit());
          }
        } else {
          int points = groupBy.get(HttpConstant.SAMPLING_POINTS).getAsInt();
          long samplingPointsUnit = Math.abs(to - from) / points;
          queryOp.setUnit(samplingPointsUnit);
          queryOp.setSlidingStep(samplingPointsUnit);
        }

        if (fills != null && (step == null || step.equals(""))) {
          queryOp.setFill(true);
          Map<TSDataType, IFill> fillTypes = new EnumMap<>(TSDataType.class);
          for (JsonElement o : fills) {
            JsonObject fill = o.getAsJsonObject();
            long duration = parseDuration(fill.get(HttpConstant.DURATION).getAsString());
            PreviousFill previousFill;
            if (fill.get(HttpConstant.PREVIOUS).getAsString()
                .equals(HttpConstant.PREVIOUS_UNTIL_LAST)) {
              previousFill = new PreviousFill(duration, true);
            } else {
              previousFill = new PreviousFill(duration);
            }
            fillTypes.put(TSDataType.valueOf(fill.get(HttpConstant.DATATYPE).getAsString()),
                previousFill);
          }
          queryOp.setFillTypes(fillTypes);
        }
      } else {
        throw new QueryProcessException("Aggregation function must have group by");
      }
    }

    int maxDeduplicatedPathNum = QueryResourceManager.getInstance()
        .getMaxDeduplicatedPathNum(1024);
    QueryPlan plan = (QueryPlan) processor.logicalPlanToPhysicalPlan(queryOp, maxDeduplicatedPathNum);
    if (!AuthorityChecker.check(username, plan.getPaths(), plan.getOperatorType(), null)) {
      throw new AuthException(
          String.format("%s can't be queried by %s", plan.getPaths(), username));
    }
    JsonArray result = new JsonArray();

    QueryDataSet dataSet = executor.processQuery(plan,
        new QueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, maxDeduplicatedPathNum)));
    List<PartialPath> paths = plan.getPaths();

    for (PartialPath path : paths) {
      JsonObject series = new JsonObject();
      JsonArray dataPoints = new JsonArray();
      series.addProperty(HttpConstant.TARGET, path.toString());
      series.add(HttpConstant.POINTS, dataPoints);
      result.add(series);
    }

    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      List<Field> fields = rowRecord.getFields();
      for (int i = 0; i < fields.size(); i++) {
        JsonObject series = result.get(i).getAsJsonObject();
        JsonArray dataPoints = series.getAsJsonArray(HttpConstant.POINTS);
        JsonArray valuesAndTime = new JsonArray();
        switch (fields.get(i).getDataType()) {
          case TEXT:
            valuesAndTime.add(fields.get(i).getBinaryV().getStringValue());
            valuesAndTime.add(rowRecord.getTimestamp());
            dataPoints.add(valuesAndTime);
            break;
          case FLOAT:
            valuesAndTime.add(fields.get(i).getFloatV());
            valuesAndTime.add(rowRecord.getTimestamp());
            dataPoints.add(valuesAndTime);
            break;
          case INT32:
            valuesAndTime.add(fields.get(i).getIntV());
            valuesAndTime.add(rowRecord.getTimestamp());
            dataPoints.add(valuesAndTime);
            break;
          case INT64:
            valuesAndTime.add(fields.get(i).getLongV());
            valuesAndTime.add(rowRecord.getTimestamp());
            dataPoints.add(valuesAndTime);
            break;
          case BOOLEAN:
            valuesAndTime.add(fields.get(i).getBoolV());
            valuesAndTime.add(rowRecord.getTimestamp());
            dataPoints.add(valuesAndTime);
            break;
          case DOUBLE:
            valuesAndTime.add(fields.get(i).getDoubleV());
            valuesAndTime.add(rowRecord.getTimestamp());
            dataPoints.add(valuesAndTime);
            break;
          default:
            throw new QueryProcessException("didn't support this datatype");
        }
      }
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
