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

public class MAFill extends ValueFill {
  int window_size = 5;
  double window_sum = 0;
  int window_cnt = 0;
  int l = 0;
  int r = window_size - 1;

  public MAFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
  }

  @Override
  public void fill() {
    for (int i = l; i < r && i < original.length; i++) {
      if (!Double.isNaN(original[i])) {
        window_sum += original[i];
        window_cnt += 1;
      }
    }
    for (int i = 0; i < original.length; i++) {
      if (!Double.isNaN(original[i])) {
        repaired[i] = original[i];
      } else {
        repaired[i] = window_sum / window_cnt;
      }
      if (i <= (window_size - 1) / 2 || i >= original.length - (window_size - 1) / 2 - 1) continue;
      if (!Double.isNaN(original[r])) {
        window_sum += original[r];
        window_cnt += 1;
      }
      l += 1;
      r += 1;
    }
  }
}
