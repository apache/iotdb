/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.operator.aggregation;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class ExtremeAccumulator implements Accumulator {

  private TsPrimitiveType extremeResult;
  private boolean hasCandidateResult;

  public ExtremeAccumulator(TSDataType seriesDataType) {
    this.extremeResult = TsPrimitiveType.getByType(seriesDataType);
  }

  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    TimeColumn timeColumn = (TimeColumn) column[0];
    for (int i = 0; i < timeColumn.getPositionCount(); i++) {
      long curTime = timeColumn.getLong(i);
      if (curTime >= timeRange.getMax() || curTime < timeRange.getMin()) {
        break;
      }
      updateResult((Comparable<Object>) column[1].getObject(i));
    }
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 1) {
      throw new IllegalArgumentException("partialResult of ExtremeValue should be 1");
    }
    updateResult((Comparable<Object>) partialResult[0].getObject(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();

    Comparable<Object> absMaxVal = (Comparable<Object>) getAbsValue(maxVal);
    Comparable<Object> absMinVal = (Comparable<Object>) getAbsValue(minVal);

    Comparable<Object> extVal = absMaxVal.compareTo(absMinVal) >= 0 ? maxVal : minVal;
    updateResult(extVal);
  }

  @Override
  public void setFinal(Column finalResult) {
    extremeResult.setObject(finalResult.getObject(0));
  }

  // columnBuilder should be single in ExtremeAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeObject(extremeResult.getValue());
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeObject(extremeResult.getValue());
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    extremeResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {extremeResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return extremeResult.getDataType();
  }

  public Object getAbsValue(Object v) {
    switch (extremeResult.getDataType()) {
      case DOUBLE:
        return Math.abs((Double) v);
      case FLOAT:
        return Math.abs((Float) v);
      case INT32:
        return Math.abs((Integer) v);
      case INT64:
        return Math.abs((Long) v);
      default:
        throw new UnSupportedDataTypeException(String.valueOf(extremeResult.getDataType()));
    }
  }

  private void updateResult(Comparable<Object> extVal) {
    if (extVal == null) {
      return;
    }

    Comparable<Object> absExtVal = (Comparable<Object>) getAbsValue(extVal);
    Comparable<Object> candidateResult = (Comparable<Object>) extremeResult.getValue();
    Comparable<Object> absCandidateResult =
        (Comparable<Object>) getAbsValue(extremeResult.getValue());

    if (!hasCandidateResult
        || (absExtVal.compareTo(absCandidateResult) > 0
            || (absExtVal.compareTo(absCandidateResult) == 0
                && extVal.compareTo(candidateResult) > 0))) {
      hasCandidateResult = true;
      extremeResult.setObject(extVal);
    }
  }
}
