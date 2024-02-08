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

package org.apache.iotdb.db.query.udf.core.access;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;

import java.io.IOException;

public class ElasticSerializableRowRecordListBackedMultiColumnWindowIterator
    implements RowIterator {

  private final ElasticSerializableRowRecordList rowRecordList;
  private final int beginIndex;
  private final int size;

  private final ElasticSerializableRowRecordListBackedMultiColumnRow row;
  private int rowIndex;

  public ElasticSerializableRowRecordListBackedMultiColumnWindowIterator(
      ElasticSerializableRowRecordList rowRecordList, int beginIndex, int endIndex) {
    this.rowRecordList = rowRecordList;
    this.beginIndex = beginIndex;
    size = endIndex - beginIndex;

    row = new ElasticSerializableRowRecordListBackedMultiColumnRow(rowRecordList.getDataTypes());
    rowIndex = -1;
  }

  @Override
  public boolean hasNextRow() {
    return rowIndex < size - 1;
  }

  @Override
  public Row next() throws IOException {
    return row.setRowRecord(rowRecordList.getRowRecord(++rowIndex + beginIndex));
  }

  @Override
  public void reset() {
    rowIndex = -1;
  }
}
