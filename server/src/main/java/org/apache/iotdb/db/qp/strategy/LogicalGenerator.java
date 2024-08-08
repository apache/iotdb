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
package org.apache.iotdb.db.qp.strategy;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.plan.expression.ResultColumn;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromComponent;
import org.apache.iotdb.db.qp.logical.crud.LastQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectComponent;
import org.apache.iotdb.db.qp.logical.crud.WhereComponent;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;

import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TIME;

/** LogicalGenerator. */
public class LogicalGenerator {

  public static Operator generate(
      String sql, ZoneId zoneId, IoTDBConstant.ClientVersion clientVersion)
      throws ParseCancellationException {
    throw new UnsupportedOperationException();
  }

  public static Operator generate(TSRawDataQueryReq rawDataQueryReq, ZoneId zoneId)
      throws IllegalPathException {
    // construct query operator and set its global time filter
    QueryOperator queryOp = new QueryOperator();
    FromComponent fromOp = new FromComponent();
    SelectComponent selectOp = new SelectComponent(zoneId);

    // iterate the path list and add it to from operator
    for (String p : rawDataQueryReq.getPaths()) {
      PartialPath path = new PartialPath(p);
      fromOp.addPrefixTablePath(path);
    }
    selectOp.addResultColumn(new ResultColumn(new TimeSeriesOperand(new PartialPath("", false))));

    queryOp.setSelectComponent(selectOp);
    queryOp.setFromComponent(fromOp);

    // set time filter operator
    FilterOperator filterOp = new FilterOperator(FilterType.KW_AND);
    PartialPath timePath = new PartialPath(TIME, false);
    filterOp.setSinglePath(timePath);
    Set<PartialPath> pathSet = new HashSet<>();
    pathSet.add(timePath);
    filterOp.setIsSingle(true);
    filterOp.setPathSet(pathSet);

    BasicFunctionOperator left =
        new BasicFunctionOperator(
            FilterType.GREATERTHANOREQUALTO,
            timePath,
            Long.toString(rawDataQueryReq.getStartTime()));
    BasicFunctionOperator right =
        new BasicFunctionOperator(
            FilterType.LESSTHAN, timePath, Long.toString(rawDataQueryReq.getEndTime()));
    filterOp.addChildOperator(left);
    filterOp.addChildOperator(right);

    queryOp.setWhereComponent(new WhereComponent(filterOp));

    return queryOp;
  }

  public static Operator generate(TSLastDataQueryReq req, ZoneId zoneId)
      throws IllegalPathException {
    // construct query operator and set its global time filter
    LastQueryOperator queryOp = new LastQueryOperator();
    FromComponent fromOp = new FromComponent();
    SelectComponent selectOp = new SelectComponent(zoneId);

    selectOp.addResultColumn(new ResultColumn(new TimeSeriesOperand(new PartialPath("", false))));

    for (String p : req.getPaths()) {
      PartialPath path = new PartialPath(p);
      fromOp.addPrefixTablePath(path);
    }

    queryOp.setSelectComponent(selectOp);
    queryOp.setFromComponent(fromOp);

    PartialPath timePath = new PartialPath(TIME, false);

    BasicFunctionOperator basicFunctionOperator =
        new BasicFunctionOperator(
            FilterType.GREATERTHANOREQUALTO, timePath, Long.toString(req.getTime()));
    queryOp.setWhereComponent(new WhereComponent(basicFunctionOperator));

    return queryOp;
  }

  @TestOnly
  public static Operator generate(String sql, ZoneId zoneId) throws ParseCancellationException {
    return generate(sql, zoneId, IoTDBConstant.ClientVersion.V_0_13);
  }

  private LogicalGenerator() {}
}
