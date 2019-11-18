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

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IEngineQueryRouter {

  /**
   * Execute physical plan.
   */
  QueryDataSet query(QueryExpression queryExpression, QueryContext context)
      throws StorageEngineException, PathErrorException;

  /**
   * Execute aggregation query.
   */
  QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, IOException, PathErrorException, ProcessorException;

  /**
   * Execute groupBy query.
   *
   * @param selectedSeries select path list
   * @param aggres aggregation name list
   * @param expression filter expression
   * @param unit time granularity for interval partitioning, unit is ms.
   * @param slidingStep  the time sliding step, unit is ms
   */
  QueryDataSet groupBy(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, long unit, long slidingStep, long startTime, long endTime,
      QueryContext context)
      throws ProcessorException, QueryFilterOptimizationException, StorageEngineException,
      PathErrorException, IOException;
  /**
   * Execute fill query.
   *
   * @param fillPaths select path list
   * @param queryTime timestamp
   * @param fillType type IFill map
   */
  QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType,
      QueryContext context) throws StorageEngineException, PathErrorException, IOException;

}
