/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.utils.cte;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.List;

public class CteDataStore {
  private final Query query;
  private final TableSchema tableSchema;

  private final List<TsBlock> cachedData;
  private long cachedBytes;
  private int cachedRows;

  public CteDataStore(Query query, TableSchema tableSchema) {
    this.query = query;
    this.tableSchema = tableSchema;
    this.cachedData = new ArrayList<>();
    this.cachedBytes = 0L;
    this.cachedRows = 0;
  }

  public boolean addTsBlock(TsBlock tsBlock) {
    IoTDBConfig iotConfig = IoTDBDescriptor.getInstance().getConfig();
    long bytesSize = tsBlock.getRetainedSizeInBytes();
    int rows = tsBlock.getPositionCount();
    if (bytesSize + cachedBytes >= iotConfig.getCteBufferSize()
        || rows + cachedRows >= iotConfig.getMaxRowsInCteBuffer()) {
      cachedData.clear();
      cachedBytes = 0;
      cachedRows = 0;
      return false;
    }
    cachedData.add(tsBlock);
    cachedBytes += bytesSize;
    cachedRows += rows;
    return true;
  }

  public void clear() {
    cachedData.clear();
    cachedBytes = 0L;
  }

  public List<TsBlock> getCachedData() {
    return cachedData;
  }

  public long getCachedBytes() {
    return cachedBytes;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public Query getQuery() {
    return query;
  }
}
