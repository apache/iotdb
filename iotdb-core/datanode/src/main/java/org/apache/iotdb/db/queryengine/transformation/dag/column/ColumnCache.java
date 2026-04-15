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

package org.apache.iotdb.db.queryengine.transformation.dag.column;

import org.apache.tsfile.block.column.Column;

import static com.google.common.base.Preconditions.checkArgument;

public class ColumnCache {

  private int referenceCount;
  private Column column;

  public ColumnCache() {
    // do nothing
  }

  public Column getColumn() {
    referenceCount--;
    checkArgument(referenceCount >= 0, "Exceed max call times of getColumn");
    Column res = this.column;
    // set column to null for memory control
    if (referenceCount == 0) {
      this.column = null;
    }
    return res;
  }

  public void clear() {
    this.column = null;
  }

  public int getPositionCount() {
    return column != null ? column.getPositionCount() : 0;
  }

  public void cacheColumn(Column column, int referenceCount) {
    this.referenceCount = referenceCount;
    this.column = column;
  }

  public boolean hasCached() {
    return column != null;
  }
}
