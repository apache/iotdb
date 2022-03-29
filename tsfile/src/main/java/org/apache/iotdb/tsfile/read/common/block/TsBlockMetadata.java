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
package org.apache.iotdb.tsfile.read.common.block;

import java.util.List;

public class TsBlockMetadata {
  // list of all columns in current TsBlock
  // The column list not only contains the series column, but also contains other column to
  // construct the final result
  // set such as timestamp and deviceName
  private List<String> columnList;

  // Indicate whether the result set should be aligned by device. This parameter can be used for
  // downstream operators
  // when processing data from current TsBlock. The RowRecord produced by TsBlock with
  // `alignedByDevice = true` will contain
  // n + 2 fields which are n series field, 1 deviceName field and 1 timestamp.
  // For example, when the FilterOperator execute the filter operation, it may need the deviceName
  // field when matching
  // the series with corresponding column in Tablet
  //
  // If alignedByDevice is true, the owned series should belong to one device
  private boolean alignedByDevice;
}
