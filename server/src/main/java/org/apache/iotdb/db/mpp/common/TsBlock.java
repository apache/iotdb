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
package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * Intermediate result for most of ExecOperators. The Tablet contains data from one or more columns
 * and constructs them as a row based view The columns can be series, aggregation result for one
 * series or scalar value (such as deviceName). The Tablet also contains the metadata to describe
 * the columns.
 *
 * <p>TODO: consider the detailed data store model in memory. (using column based or row based ?)
 */
public class TsBlock {

  // Describe the column info
  private TsBlockMetadata metadata;

  public boolean hasNext() {
    return false;
  }

  // Get next row in current tablet
  public RowRecord getNext() {
    return null;
  }

  public TsBlockMetadata getMetadata() {
    return metadata;
  }
}
