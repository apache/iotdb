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

package org.apache.iotdb.db.mpp.transformation.dag.input;

import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.List;

/** The input data set interface for a UDFPlan */
public interface IUDFInputDataSet {

  /** returns the input data types, except the timestamp column(the last column). */
  List<TSDataType> getDataTypes();

  /** Whether the data set has next row. */
  boolean hasNextRowInObjects() throws IOException;

  /** Whether the data set has next row. */
  default YieldableState canYieldNextRowInObjects() throws IOException {
    return hasNextRowInObjects()
        ? YieldableState.YIELDABLE
        : YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
  }

  /**
   * Get the next row for UDFPlan input.
   *
   * <p>The last element in the row is the timestamp.
   */
  Object[] nextRowInObjects() throws IOException;
}
