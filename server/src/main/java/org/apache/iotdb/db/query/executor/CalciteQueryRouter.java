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

package org.apache.iotdb.db.query.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByLevelDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Query entrance class of IoTDB query process. All query clause will be transformed to physical
 * plan, physical plan will be executed by EngineQueryRouter.
 */
public class CalciteQueryRouter implements IQueryRouter {

  private static Logger logger = LoggerFactory.getLogger(CalciteQueryRouter.class);

  @Override
  public QueryDataSet rawDataQuery(RawDataQueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    CalciteExecutor executor = new CalciteExecutor();

    RelNode relNode = executor.toRelNode(queryPlan);

    QueryDataSet myQuerySet = executor.execute(context, relNode);

    return myQuerySet;
  }

  @Override
  public QueryDataSet aggregate(AggregationPlan aggregationPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
      IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryDataSet udafQuery(UDAFPlan udafPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, IOException,
      QueryProcessException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryDataSet groupBy(GroupByTimePlan groupByTimePlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
      IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryDataSet fill(FillQueryPlan fillQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryDataSet groupByFill(GroupByTimeFillPlan groupByFillPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryDataSet lastQuery(LastQueryPlan lastQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryDataSet udtfQuery(UDTFPlan udtfPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }
}
