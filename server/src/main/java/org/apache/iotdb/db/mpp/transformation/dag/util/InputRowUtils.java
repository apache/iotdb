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
package org.apache.iotdb.db.mpp.transformation.dag.util;

import org.apache.iotdb.db.mpp.transformation.dag.input.IUDFInputDataSet;

public class InputRowUtils {

  /**
   * this method checks whether the row returned by IUDFInputDataSet.nextRowInObjects() has all null
   * fields except the timestamp
   *
   * @param row the returned row by calling {@link IUDFInputDataSet#nextRowInObjects()}
   * @return true if all row fields are null.
   */
  public static boolean isAllNull(Object[] row) {
    if (row == null) {
      return true;
    }
    for (int i = 0; i < row.length - 1; i++) {
      if (row[i] != null) {
        return false;
      }
    }
    return true;
  }

  /**
   * this method checks whether the row returned by IUDFInputDataSet.nextRowInObjects() has any null
   * fields except the timestamp
   *
   * @param row the returned row by calling {@link IUDFInputDataSet#nextRowInObjects()}
   * @return true if any row field is null.
   */
  public static boolean hasNullField(Object[] row) {
    if (row == null) {
      return true;
    }
    for (int i = 0; i < row.length - 1; i++) {
      if (row[i] == null) {
        return true;
      }
    }
    return false;
  }
}
