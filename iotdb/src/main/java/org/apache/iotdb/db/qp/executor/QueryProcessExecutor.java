/**
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
package org.apache.iotdb.db.qp.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.query.executor.IEngineQueryRouter;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public abstract class QueryProcessExecutor implements IQueryProcessExecutor {

  protected ThreadLocal<Integer> fetchSize = new ThreadLocal<>();
  protected IEngineQueryRouter queryRouter = new EngineQueryRouter();

  @Override
  public QueryDataSet processQuery(QueryPlan queryPlan, QueryContext context)
      throws IOException, FileNodeManagerException, PathErrorException,
      QueryFilterOptimizationException, ProcessorException {

    QueryExpression queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    if (queryPlan instanceof GroupByPlan) {
      GroupByPlan groupByPlan = (GroupByPlan) queryPlan;
      return groupBy(groupByPlan.getPaths(), groupByPlan.getAggregations(),
          groupByPlan.getExpression(), groupByPlan.getUnit(), groupByPlan.getOrigin(),
          groupByPlan.getIntervals(), context);
    }

    if (queryPlan instanceof AggregationPlan) {
      return aggregate(queryPlan.getPaths(), queryPlan.getAggregations(),
          queryPlan.getExpression(), context);
    }

    if (queryPlan instanceof FillQueryPlan) {
      FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
      return fill(queryPlan.getPaths(), fillQueryPlan.getQueryTime(),
          fillQueryPlan.getFillType(), context);
    }
    return queryRouter.query(queryExpression, context);
  }

  @Override
  public int getFetchSize() {
    if (fetchSize.get() == null) {
      return 100;
    }
    return fetchSize.get();
  }

  @Override
  public void setFetchSize(int fetchSize) {
    this.fetchSize.set(fetchSize);
  }

  @Override
  public boolean delete(List<Path> paths, long deleteTime) throws ProcessorException {
    try {
      boolean result = true;
      MManager mManager = MManager.getInstance();
      Set<String> pathSet = new HashSet<>();
      for (Path p : paths) {
        pathSet.addAll(mManager.getPaths(p.getFullPath()));
      }
      if (pathSet.isEmpty()) {
        throw new ProcessorException("TimeSeries does not exist and cannot be delete data");
      }
      for (String onePath : pathSet) {
        if (!mManager.pathExist(onePath)) {
          throw new ProcessorException(
              String.format("TimeSeries %s does not exist and cannot be delete its data", onePath));
        }
      }
      List<String> fullPath = new ArrayList<>();
      fullPath.addAll(pathSet);
      for (String path : fullPath) {
        result &= delete(new Path(path), deleteTime);
      }
      return result;
    } catch (PathErrorException e) {
      throw new ProcessorException(e.getMessage());
    }
  }

}
