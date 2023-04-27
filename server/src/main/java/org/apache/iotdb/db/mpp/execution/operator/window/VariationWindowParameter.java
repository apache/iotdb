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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class VariationWindowParameter extends WindowParameter {

  private final boolean ignoringNull;
  private final double delta;
  private final TSDataType dataType;

  private final int controlColumnIndex;

  public VariationWindowParameter(
      TSDataType dataType,
      int controlColumnIndex,
      boolean needOutputEndTime,
      boolean ignoringNull,
      double delta) {
    super(needOutputEndTime);
    this.controlColumnIndex = controlColumnIndex;
    this.dataType = dataType;
    this.ignoringNull = ignoringNull;
    this.delta = delta;
    this.windowType = WindowType.VARIATION_WINDOW;
  }

  public boolean isIgnoringNull() {
    return ignoringNull;
  }

  public double getDelta() {
    return delta;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public int getControlColumnIndex() {
    return controlColumnIndex;
  }
}
