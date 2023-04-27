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

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.BitMap;

public class LastValueDescAccumulator extends LastValueAccumulator {

  public LastValueDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public boolean hasFinalResult() {
    return initResult;
  }

  @Override
  public void reset() {
    super.reset();
  }

  @Override
  protected void addIntInput(Column[] column, BitMap needSkip, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateIntLastValue(column[1].getInt(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addLongInput(Column[] column, BitMap needSkip, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateLongLastValue(column[1].getLong(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addFloatInput(Column[] column, BitMap needSkip, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateFloatLastValue(column[1].getFloat(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addDoubleInput(Column[] column, BitMap needSkip, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateDoubleLastValue(column[1].getDouble(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addBooleanInput(Column[] column, BitMap needSkip, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBooleanLastValue(column[1].getBoolean(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addBinaryInput(Column[] column, BitMap needSkip, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBinaryLastValue(column[1].getBinary(i), column[0].getLong(i));
        return;
      }
    }
  }
}
