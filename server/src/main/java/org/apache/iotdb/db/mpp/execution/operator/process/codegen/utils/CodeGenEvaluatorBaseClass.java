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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils;

import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.udf.api.access.Row;

import java.util.List;
import java.util.Map;

public abstract class CodeGenEvaluatorBaseClass {

  protected Map<String, Boolean> isNull;

  protected TsBlock inputTsBlock;

  protected long timestamp;

  protected List<TSDataType> outputDataTypes;

  protected TsBlockBuilder tsBlockBuilder;

  protected TimeColumn timeColumn;
  protected Column[] valueColumns;
  protected ColumnBuilder[] outputColumns;
  protected List<Boolean> outputExpressionGenerateSuccess;

  protected UDTFExecutor[] executors;

  protected CodegenSimpleRow[] rows;

  public void setIsNull(Map<String, Boolean> isNull) {
    this.isNull = isNull;
  }

  public void setOutputExpressionGenerateSuccess(List<Boolean> outputExpressionGenerateSuccess) {
    this.outputExpressionGenerateSuccess = outputExpressionGenerateSuccess;
  }

  public static Object udtfCall(UDTFExecutor udtfExecutor, Row row) {
    udtfExecutor.execute(row);
    return udtfExecutor.getCurrentValue();
  }

  public void setOutputDataTypes(List<TSDataType> outputDataTypes) {
    this.outputDataTypes = outputDataTypes;
  }

  protected void resetOutputTsBlock() {
    if (tsBlockBuilder == null) {
      tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    } else {
      tsBlockBuilder.reset();
    }
    outputColumns = tsBlockBuilder.getValueColumnBuilders();
  }

  protected void resetColumns() {
    valueColumns = inputTsBlock.getValueColumns();
    timeColumn = inputTsBlock.getTimeColumn();
  }

  public Column[] evaluate(TsBlock inputTsBlock) {
    this.inputTsBlock = inputTsBlock;

    resetOutputTsBlock();
    resetColumns();
    int positionCount = inputTsBlock.getPositionCount();
    for (int i = 0; i < positionCount; i++) {
      timestamp = timeColumn.getLong(i);
      evaluateByRow(i);
    }
    tsBlockBuilder.declarePositions(positionCount);

    Column[] output = new Column[outputColumns.length];
    for (int i = 0; i < output.length; ++i) {
      if (outputExpressionGenerateSuccess.get(i)) {
        output[i] = outputColumns[i].build();
      }
    }

    return output;
  }

  protected abstract void evaluateByRow(int i);

  protected abstract void updateInputVariables(int i);

  public void setExecutors(UDTFExecutor[] executors) {
    this.executors = executors;
  }

  public void setRows(CodegenSimpleRow[] rows) {
    this.rows = rows;
  }
}
