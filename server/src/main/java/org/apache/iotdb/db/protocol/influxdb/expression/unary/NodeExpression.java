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
package org.apache.iotdb.db.protocol.influxdb.expression.unary;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NodeExpression extends Expression {

  protected String name;

  public NodeExpression(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {

  }

  @Override
  public void removeWildcards(WildcardsRemover wildcardsRemover, List<Expression> resultExpressions) throws LogicalOptimizeException {

  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {

  }

  @Override
  public void constructUdfExecutors(Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {

  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {

  }

  @Override
  public IntermediateLayer constructIntermediateLayer(
          long queryId,
          UDTFPlan udtfPlan,
          RawQueryInputLayer rawTimeSeriesInputLayer,
          Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
          Map<Expression, TSDataType> expressionDataTypeMap,
          LayerMemoryAssigner memoryAssigner)
          throws QueryProcessException, IOException {
    return null;
  }

  @Override
  protected boolean isConstantOperandInternal() {
    return isConstantOperandCache;
  }

  @Override
  protected String getExpressionStringInternal() {
    return "Node name: " + name;
  }
}
