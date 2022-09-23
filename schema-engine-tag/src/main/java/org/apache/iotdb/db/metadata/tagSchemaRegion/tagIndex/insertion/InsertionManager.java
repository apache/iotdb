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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALManager;
import org.apache.iotdb.lsm.context.InsertRequestContext;
import org.apache.iotdb.lsm.manager.BasicLsmManager;

import java.io.IOException;

/** manage insertion to MemTable */
public class InsertionManager extends BasicLsmManager<MemTable, InsertRequestContext> {

  // use wal manager object to write wal file on insertion
  private WALManager walManager;

  public InsertionManager(WALManager walManager) {
    this.walManager = walManager;
    initLevelProcess();
  }

  /**
   * write wal file on insertion
   *
   * @param root root memory node
   * @param context insert request context
   * @throws Exception
   */
  @Override
  public void preProcess(MemTable root, InsertRequestContext context) throws IOException {
    if (!context.isRecover()) {
      walManager.write(context);
    }
  }

  /** set the insert operation for each layer of memory nodes */
  private void initLevelProcess() {
    this.nextLevel(new MemTableInsertion())
        .nextLevel(new MemChunkGroupInsertion())
        .nextLevel(new MemChunkInsertion());
  }
}
