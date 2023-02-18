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

import org.apache.iotdb.db.engine.memtable.IMemTable;

import java.io.IOException;

public interface FlushListener {
  void onMemTableFlushStarted(IMemTable memTable) throws IOException;

  void onMemTableFlushed(IMemTable memTable);

  class DefaultMemTableFLushListener implements FlushListener {

    public static final DefaultMemTableFLushListener INSTANCE = new DefaultMemTableFLushListener();

    @Override
    public void onMemTableFlushStarted(IMemTable memTable) {
      memTable.setFlushStatus(FlushStatus.FLUSHING);
    }

    @Override
    public void onMemTableFlushed(IMemTable memTable) {
      memTable.setFlushStatus(FlushStatus.FLUSHED);
    }
  }
}
