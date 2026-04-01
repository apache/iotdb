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

package org.apache.iotdb.db.queryengine.transformation.datastructure;

import org.apache.tsfile.block.column.Column;

public class TVColumns {
  private boolean isConstant;
  private Column timeColumn;
  private Column valueColumn;

  public TVColumns(Column timeColumn, Column valueColumn) {
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    isConstant = false;
  }

  public TVColumns(Column valueColumn) {
    this.valueColumn = valueColumn;
    isConstant = true;
  }

  public int getPositionCount() {
    // In case of constant, use valueColumn to get pos count
    return valueColumn.getPositionCount();
  }

  public long getTimeByIndex(int index) {
    if (isConstant) {
      throw new UnsupportedOperationException();
    }
    return timeColumn.getLong(index);
  }

  public long getEndTime() {
    if (isConstant) {
      throw new UnsupportedOperationException();
    }
    return timeColumn.getLong(timeColumn.getPositionCount() - 1);
  }

  public Column getValueColumn() {
    return valueColumn;
  }

  public Column getTimeColumn() {
    if (isConstant) {
      throw new UnsupportedOperationException();
    }
    return timeColumn;
  }

  public boolean isConstant() {
    return isConstant;
  }
}
