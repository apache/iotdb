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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.udf.core.UDFContext;
import org.apache.iotdb.db.query.udf.core.UDTFExecutor;

public class UDTFPlan extends RawDataQueryPlan implements UDFPlan {

  /**
   * each udf query column has an executor instance
   * <p>
   * this.pathToIndex + the column name recorded in UDTFExecutor -> the index of the udf query
   * output column in UDTFDataSet
   */
  protected List<UDTFExecutor> deduplicatedExecutors;

  /**
   * the original output column index (not the output column index in the DataSet) -> the executor
   * in deduplicatedExecutors
   * <p>
   * outputColumnIndex2DeduplicatedExecutorIndexList[original output index] is null when the output
   * column is for a raw query
   */
  protected List<Integer> outputColumnIndex2DeduplicatedExecutorIndexList;

  /**
   * rawQueryReaderIndex2DataSetOutputColumnIndexList is for raw query columns
   * <p>
   * the index of a series reader (for a raw query) in UDTFDataSet -> the index of the raw query
   * output column in UDTFDataSet
   * <p>
   * we can not use the full series path name of the reader to get (use this.pathToIndex) the index
   * of the raw query output column in UDTFDataSet, because a series may have two or more readers
   * (for example, one is for a raw query, and the other is for a udf query)
   */
  protected List<Integer> rawQueryReaderIndex2DataSetOutputColumnIndexList;

  /**
   * used to generate temporary query file paths
   */
  protected List<String> seriesReaderIdList;

  public UDTFPlan() {
    super();
    setOperatorType(Operator.OperatorType.UDTF);
    deduplicatedExecutors = new ArrayList<>();
    outputColumnIndex2DeduplicatedExecutorIndexList = new ArrayList<>();
    rawQueryReaderIndex2DataSetOutputColumnIndexList = new ArrayList<>();
    seriesReaderIdList = new ArrayList<>();
  }

  @Override
  public void initializeUdfExecutors(List<UDFContext> udfContexts) throws QueryProcessException {
    // the column name of a udf query -> the executor in deduplicatedExecutors
    Map<String, Integer> outputColumn2DeduplicatedExecutorIndexMap = new HashMap<>();

    for (UDFContext context : udfContexts) {
      if (context == null) {
        outputColumnIndex2DeduplicatedExecutorIndexList.add(null);
        continue;
      }

      String column = context.getColumn();
      Integer index = outputColumn2DeduplicatedExecutorIndexMap.get(column);
      if (index != null) {
        outputColumnIndex2DeduplicatedExecutorIndexList.add(index);
        continue;
      }

      index = deduplicatedExecutors.size();
      outputColumnIndex2DeduplicatedExecutorIndexList.add(index);
      UDTFExecutor executor = new UDTFExecutor(context);
      executor.initializeUDF();
      deduplicatedExecutors.add(executor);
      outputColumn2DeduplicatedExecutorIndexMap.put(column, index);
    }
  }

  @Override
  public void finalizeUDFExecutors() {
    for (UDTFExecutor executor : deduplicatedExecutors) {
      executor.finalizeUDF();
    }
  }

  public List<UDTFExecutor> getDeduplicatedExecutors() {
    return deduplicatedExecutors;
  }

  public UDTFExecutor getExecutor(int originalColumnIndex) {
    Integer executorIndex = outputColumnIndex2DeduplicatedExecutorIndexList
        .get(originalColumnIndex);
    return executorIndex == null ? null : deduplicatedExecutors.get(executorIndex);
  }

  public void addRawQueryReaderIndex2DataSetOutputColumnIndex(Integer outputIndex) {
    rawQueryReaderIndex2DataSetOutputColumnIndexList.add(outputIndex);
  }

  public List<Integer> getRawQueryReaderIndex2DataSetOutputColumnIndexList() {
    return rawQueryReaderIndex2DataSetOutputColumnIndexList;
  }

  public void addSeriesReaderId(String id) {
    seriesReaderIdList.add(id);
  }

  public List<String> getSeriesReaderIdList() {
    return seriesReaderIdList;
  }
}
