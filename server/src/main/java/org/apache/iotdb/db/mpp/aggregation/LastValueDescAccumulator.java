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
import org.apache.iotdb.tsfile.utils.Binary;

public class LastValueDescAccumulator extends LastValueAccumulator {

  private boolean hasCandidateResult = false;

  public LastValueDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    super.reset();
  }

  protected void updateIntLastValue(int value, long curTime) {
    hasCandidateResult = true;
    super.updateIntLastValue(value, curTime);
  }

  protected void updateLongLastValue(long value, long curTime) {
    hasCandidateResult = true;
    super.updateLongLastValue(value, curTime);
  }

  protected void updateFloatLastValue(float value, long curTime) {
    hasCandidateResult = true;
    super.updateFloatLastValue(value, curTime);
  }

  protected void updateDoubleLastValue(double value, long curTime) {
    hasCandidateResult = true;
    super.updateDoubleLastValue(value, curTime);
  }

  protected void updateBooleanLastValue(boolean value, long curTime) {
    hasCandidateResult = true;
    super.updateBooleanLastValue(value, curTime);
  }

  protected void updateBinaryLastValue(Binary value, long curTime) {
    hasCandidateResult = true;
    super.updateBinaryLastValue(value, curTime);
  }
}
