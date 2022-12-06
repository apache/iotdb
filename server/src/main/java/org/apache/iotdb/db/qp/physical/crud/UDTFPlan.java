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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.ResultColumn;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDTFPlan extends RawDataQueryPlan implements UDFPlan {

  protected final UDTFContext udtfContext;

  protected final Map<Integer, Integer> datasetOutputIndexToResultColumnIndex = new HashMap<>();
  protected final Map<String, Integer> pathNameToReaderIndex = new HashMap<>();

  public UDTFPlan(ZoneId zoneId) {
    super();
    udtfContext = new UDTFContext(zoneId);
    setOperatorType(Operator.OperatorType.UDTF);
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public List<TSDataType> getWideQueryHeaders(
      List<String> respColumns, List<String> respSgColumns, boolean isJdbcQuery, BitSet aliasList) {
    List<TSDataType> seriesTypes = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      respColumns.add(resultColumns.get(i).getResultColumnName());
      // @Deprecated
      // seriesTypes.add(resultColumns.get(i).getDataType());
    }
    return seriesTypes;
  }

  protected void setDatasetOutputIndexToResultColumnIndex(
      int datasetOutputIndex, Integer originalIndex) {
    datasetOutputIndexToResultColumnIndex.put(datasetOutputIndex, originalIndex);
  }

  @Override
  public List<PartialPath> getAuthPaths() {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void constructUdfExecutors(List<ResultColumn> resultColumns) {
    udtfContext.constructUdfExecutors(resultColumns);
  }

  @Override
  public void finalizeUDFExecutors(long queryId) {
    udtfContext.finalizeUDFExecutors(queryId);
  }

  public ResultColumn getResultColumnByDatasetOutputIndex(int datasetOutputIndex) {
    return resultColumns.get(datasetOutputIndexToResultColumnIndex.get(datasetOutputIndex));
  }

  public Integer getReaderIndexByExpressionName(String expressionName) {
    return pathNameToReaderIndex.get(expressionName);
  }

  public UDTFContext getUdtfContext() {
    return udtfContext;
  }
}
