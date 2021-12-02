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

package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.dataset.udf.UDTFAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.udf.UDTFFragmentDataSet;
import org.apache.iotdb.db.query.dataset.udf.UDTFJoinDataSet;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LayerBuilder {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final long queryId;
  private final UDTFPlan udtfPlan;
  private final RawQueryInputLayer rawTimeSeriesInputLayer;

  // input
  private final Expression[] resultColumnExpressions;
  // output
  private final LayerPointReader[] resultColumnPointReaders;

  private final LayerMemoryAssigner memoryAssigner;

  // all result column expressions will be split into several sub-expressions, each expression has
  // its own result point reader. different result column expressions may have the same
  // sub-expressions, but they can share the same point reader. we cache the point reader here to
  // make sure that only one point reader will be built for one expression.
  private final Map<Expression, IntermediateLayer> expressionIntermediateLayerMap;
  private final Map<Expression, TSDataType> expressionDataTypeMap;

  // used to split query dataset into fragments.
  // useless when the dataset can not be split into fragments.
  private final List<List<LayerPointReader>> fragmentDataSetIndexToLayerPointReaders;
  private final int[][] resultColumnOutputIndexToFragmentDataSetOutputIndex;

  public LayerBuilder(
      long queryId, UDTFPlan udtfPlan, RawQueryInputLayer inputLayer, float memoryBudgetInMB) {
    this.queryId = queryId;
    this.udtfPlan = udtfPlan;
    this.rawTimeSeriesInputLayer = inputLayer;

    int size = udtfPlan.getPathToIndex().size();
    resultColumnExpressions = new Expression[size];
    for (int i = 0; i < size; ++i) {
      resultColumnExpressions[i] = udtfPlan.getResultColumnByDatasetOutputIndex(i).getExpression();
    }
    resultColumnPointReaders = new LayerPointReader[size];

    memoryAssigner = new LayerMemoryAssigner(memoryBudgetInMB);

    expressionIntermediateLayerMap = new HashMap<>();
    expressionDataTypeMap = new HashMap<>();

    fragmentDataSetIndexToLayerPointReaders = new ArrayList<>();
    resultColumnOutputIndexToFragmentDataSetOutputIndex = new int[resultColumnExpressions.length][];
  }

  public LayerBuilder buildLayerMemoryAssigner() {
    for (Expression expression : resultColumnExpressions) {
      expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    }
    memoryAssigner.build();
    return this;
  }

  public LayerBuilder buildResultColumnPointReaders() throws QueryProcessException, IOException {
    for (int i = 0, n = resultColumnExpressions.length; i < n; ++i) {
      // resultColumnExpressions[i] -> the index of the fragment it belongs to
      Integer fragmentDataSetIndex =
          resultColumnExpressions[i].tryToGetFragmentDataSetIndex(expressionIntermediateLayerMap);
      if (fragmentDataSetIndex == null) {
        fragmentDataSetIndex = fragmentDataSetIndexToLayerPointReaders.size();
        fragmentDataSetIndexToLayerPointReaders.add(new ArrayList<>());
      }

      // build point readers
      resultColumnPointReaders[i] =
          resultColumnExpressions[i]
              .constructIntermediateLayer(
                  queryId,
                  udtfPlan,
                  rawTimeSeriesInputLayer,
                  expressionIntermediateLayerMap,
                  expressionDataTypeMap,
                  memoryAssigner,
                  fragmentDataSetIndex)
              .constructPointReader();

      // collect layer point readers for fragments
      List<LayerPointReader> layerPointReadersInFragmentDataSet =
          fragmentDataSetIndexToLayerPointReaders.get(fragmentDataSetIndex);
      // note that expressions in resultColumnExpressions are all unique
      // see UDTFPlan#deduplicate() for more detail
      resultColumnOutputIndexToFragmentDataSetOutputIndex[i] =
          new int[] {fragmentDataSetIndex, layerPointReadersInFragmentDataSet.size()};
      layerPointReadersInFragmentDataSet.add(resultColumnPointReaders[i]);
    }
    return this;
  }

  public LayerBuilder setDataSetResultColumnDataTypes() {
    for (ResultColumn resultColumn : udtfPlan.getResultColumns()) {
      resultColumn.setDataType(expressionDataTypeMap.get(resultColumn.getExpression()));
    }
    return this;
  }

  public LayerPointReader[] getResultColumnPointReaders() {
    return resultColumnPointReaders;
  }

  public boolean canBeSplitIntoFragments() {
    return Math.min(2, CONFIG.getUdfMinFragmentNumberToTriggerParallelExecution())
        <= fragmentDataSetIndexToLayerPointReaders.size();
  }

  public QueryDataSet generateJoinDataSet(UDTFAlignByTimeDataSet udtfAlignByTimeDataSet)
      throws QueryProcessException, IOException {
    int n = fragmentDataSetIndexToLayerPointReaders.size();
    UDTFFragmentDataSet[] fragmentDataSets = new UDTFFragmentDataSet[n];
    for (int i = 0; i < n; ++i) {
      fragmentDataSets[i] =
          new UDTFFragmentDataSet(
              rawTimeSeriesInputLayer,
              fragmentDataSetIndexToLayerPointReaders.get(i).toArray(new LayerPointReader[0]));
    }

    return new UDTFJoinDataSet(
        udtfAlignByTimeDataSet,
        fragmentDataSets,
        resultColumnOutputIndexToFragmentDataSetOutputIndex);
  }
}
