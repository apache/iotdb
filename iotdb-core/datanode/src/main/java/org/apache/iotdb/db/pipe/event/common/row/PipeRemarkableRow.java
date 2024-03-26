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

package org.apache.iotdb.db.pipe.event.common.row;

public class PipeRemarkableRow extends PipeRow {

  private final boolean[] isNullMarked;

  public PipeRemarkableRow(PipeRow pipeRow) {
    super(
        pipeRow.rowIndex,
        pipeRow.deviceId,
        pipeRow.isAligned,
        pipeRow.measurementSchemaList,
        pipeRow.timestampColumn,
        pipeRow.valueColumnTypes,
        pipeRow.valueColumns,
        pipeRow.bitMaps,
        pipeRow.columnNameStringList);

    // Copy the isNullMarked array from the original row
    // to avoid modifying the original row.
    final int columnCount = pipeRow.measurementSchemaList.length;
    isNullMarked = new boolean[columnCount];
    for (int i = 0; i < columnCount; i++) {
      isNullMarked[i] = super.isNull(i);
    }
  }

  @Override
  public boolean isNull(int index) {
    return isNullMarked[index];
  }

  public void markNull(int index) {
    isNullMarked[index] = true;
  }
}
