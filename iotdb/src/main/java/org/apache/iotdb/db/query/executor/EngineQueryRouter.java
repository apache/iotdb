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

package org.apache.iotdb.db.query.executor;

import static org.apache.iotdb.tsfile.read.expression.ExpressionType.GLOBAL_TIME;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.OpenedFilePathsManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.dataset.AggregateDataSet;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * Query entrance class of IoTDB query process. All query clause will be transformed to physical
 * plan, physical plan will be executed by EngineQueryRouter.
 */
public class EngineQueryRouter {

  /**
   * Each unique jdbc request(query, aggregation or others job) has an unique job id. This job id
   * will always be maintained until the request is closed. In each job, the unique file will be
   * only opened once to avoid too many opened files error.
   */
  private AtomicLong jobIdGenerator = new AtomicLong();

  /**
   * execute physical plan.
   */
  public QueryDataSet query(QueryExpression queryExpression)
      throws FileNodeManagerException {

    long nextJobId = getNextJobId();
    QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);
    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);

    QueryContext context = new QueryContext();

    if (queryExpression.hasQueryFilter()) {
      try {
        IExpression optimizedExpression = ExpressionOptimizer.getInstance()
            .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
        queryExpression.setExpression(optimizedExpression);

        if (optimizedExpression.getType() == GLOBAL_TIME) {
          EngineExecutorWithoutTimeGenerator engineExecutor =
              new EngineExecutorWithoutTimeGenerator(

                  nextJobId, queryExpression);
          return engineExecutor.executeWithGlobalTimeFilter(context);
        } else {
          EngineExecutorWithTimeGenerator engineExecutor = new EngineExecutorWithTimeGenerator(
              nextJobId,
              queryExpression);
          return engineExecutor.execute(context);
        }

      } catch (QueryFilterOptimizationException e) {
        throw new FileNodeManagerException(e);
      }
    } else {
      EngineExecutorWithoutTimeGenerator engineExecutor = new EngineExecutorWithoutTimeGenerator(
          nextJobId,
          queryExpression);
      return engineExecutor.executeWithoutFilter(context);
    }
  }

  /**
   * execute aggregation query.
   */
  public AggregateDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression)
      throws QueryFilterOptimizationException, FileNodeManagerException, IOException, PathErrorException, ProcessorException {

    long nextJobId = getNextJobId();
    QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);
    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);

    QueryContext context = new QueryContext();

    if (expression != null) {
      IExpression optimizedExpression = ExpressionOptimizer.getInstance()
          .optimize(expression, selectedSeries);
      AggregateEngineExecutor engineExecutor = new AggregateEngineExecutor(nextJobId,
          selectedSeries, aggres, optimizedExpression);
      if (optimizedExpression.getType() == GLOBAL_TIME) {
        return engineExecutor.executeWithOutTimeGenerator(context);
      } else {
        return engineExecutor.executeWithTimeGenerator(context);
      }
    } else {
      AggregateEngineExecutor engineExecutor = new AggregateEngineExecutor(nextJobId,
          selectedSeries, aggres, expression);
      return engineExecutor.executeWithOutTimeGenerator(context);
    }
  }

  private synchronized long getNextJobId() {
    return jobIdGenerator.incrementAndGet();
  }
}
