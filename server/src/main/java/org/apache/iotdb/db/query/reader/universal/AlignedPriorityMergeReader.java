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
package org.apache.iotdb.db.query.reader.universal;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class AlignedPriorityMergeReader extends PriorityMergeReader {

  /**
   * In the case of aligned time seres, like d1.s1, d1.s2, d1.s3, we may first only write d1.s2,
   * d1.s3 (2, 3)and then flush it into one tsfile, and then an unseq record come, it contains only
   * d1.s1 and d1.s3 -> (10,30). So now we have [null, 2, 3] with low priority and [10, null, 30]
   * with high priority. we use [null, 2, 3] to fill null value in [10, null, 30], so we get [10, 2,
   * 30], we won't use 3 to replace 30, because [10, null, 30] has higher priority
   *
   * @param v TimeValuePair with high priority needs to be filled
   * @param c TimeValuePair with low priority is used to fill the v
   */
  @Override
  protected void fillNullValue(TimeValuePair v, TimeValuePair c) {
    fillNullValueInAligned(v, c);
  }

  static void fillNullValueInAligned(TimeValuePair v, TimeValuePair c) {
    TsPrimitiveType[] vArray = v.getValue().getVector();
    TsPrimitiveType[] cArray = c.getValue().getVector();
    for (int i = 0; i < vArray.length; i++) {
      if ((vArray[i] == null || vArray[i].getValue() == null)
          && (cArray[i] != null && cArray[i].getValue() != null)) {
        vArray[i] = cArray[i];
      }
    }
  }
}
