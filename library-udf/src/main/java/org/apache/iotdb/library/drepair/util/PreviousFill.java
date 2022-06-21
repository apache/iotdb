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
package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.udf.api.access.RowIterator;

public class PreviousFill extends ValueFill {

  private long previousTime = -1;
  private double previousValue;

  public PreviousFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
  }

  @Override
  public void fill() {
    // NaN at the beginning is not filled
    for (int i = 0; i < original.length; i++) {
      if (Double.isNaN(original[i])) {
        if (previousTime == -1) { // 序列初始为空，直接pass
          repaired[i] = original[i];
        } else {
          repaired[i] = previousValue;
        }
      } else {
        previousTime = time[i];
        previousValue = original[i];
        repaired[i] = original[i];
      }
    }
  }
}
