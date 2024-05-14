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

package org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator;

import org.apache.iotdb.db.queryengine.transformation.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.row.SerializableRowRecordList;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.io.IOException;
import java.util.List;

public class RowListForwardIterator implements ListForwardIterator {
  private ElasticSerializableRowRecordList rowList;

  private int externalIndex;
  private int internalIndex;

  private Column[] cachedColumns;

  public RowListForwardIterator(ElasticSerializableRowRecordList rowList) {
    this.rowList = rowList;
    // Point to dummy block
    this.externalIndex = 0;
    this.internalIndex = -1;
  }

  public RowListForwardIterator(
      ElasticSerializableRowRecordList rowList, int externalIndex, int internalIndex) {
    this.rowList = rowList;
    this.externalIndex = externalIndex;
    this.internalIndex = internalIndex;
  }

  public Column[] currentBlock() throws IOException {
    return cachedColumns == null ? rowList.getColumns(externalIndex, internalIndex) : cachedColumns;
  }

  @Override
  public boolean hasNext() {
    // First time call, rowList has no data
    List<SerializableRowRecordList> internalLists = rowList.getInternalRowList();
    if (internalLists.size() == 0) {
      return false;
    }
    return externalIndex + 1 < internalLists.size()
        || internalIndex + 1 < internalLists.get(externalIndex).getBlockCount();
  }

  @Override
  public void next() throws IOException {
    List<SerializableRowRecordList> internalLists = rowList.getInternalRowList();
    if (internalIndex + 1 == internalLists.get(externalIndex).getBlockCount()) {
      internalIndex = 0;
      externalIndex++;
    } else {
      internalIndex++;
    }

    cachedColumns = rowList.getColumns(externalIndex, internalIndex);
  }

  public void adjust() throws IOException {
    RowListForwardIterator iterator = rowList.constructIterator();

    while (iterator.hasNext()) {
      iterator.next();
      Column[] columns = iterator.currentBlock();
      if (columns == cachedColumns) {
        this.externalIndex = iterator.externalIndex;
        this.internalIndex = iterator.internalIndex;
        break;
      }
    }
  }
}
