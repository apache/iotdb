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
package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.db.engine.memtable.AbstractMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

/**
 * Only used in sync flush and async close to start a flush task This memtable is not managed by
 * MemTablePool and does not store any data.
 */
public class NotifyFlushMemTable extends AbstractMemTable {

  @Override
  protected IWritableMemChunk genMemSeries(IMeasurementSchema schema) {
    return null;
  }

  @Override
  public IMemTable copy() {
    return null;
  }

  @Override
  public boolean isSignalMemTable() {
    return true;
  }
}
