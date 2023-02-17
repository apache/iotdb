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

public class LinearFill extends ValueFill {

  private int prevNotNaN = -1;

  public LinearFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
  }

  @Override
  public void fill() {
    for (int i = 0; i < original.length; i++) {
      if (!Double.isNaN(original[i])) {
        double k = 0;
        if (prevNotNaN > 0) {
          k = original[i] - original[prevNotNaN];
          k /= i - prevNotNaN;
        }
        int t = prevNotNaN + 1;
        while (t < i) {
          repaired[t] = original[i] + k * (t - i);
          t++;
        }
        repaired[i] = original[i];
        prevNotNaN = i;
      }
    }
    if (prevNotNaN < original.length - 1 && prevNotNaN >= 0) {
      int t = prevNotNaN;
      while (t <= original.length - 1) {
        repaired[t] = original[prevNotNaN];
        t++;
      }
    }
  }
}
