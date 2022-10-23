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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALManager;
import org.apache.iotdb.lsm.context.DeleteRequestContext;
import org.apache.iotdb.lsm.levelProcess.LevelProcessChain;
import org.apache.iotdb.lsm.manager.BasicLSMManager;

/** manage deletion to MemTable */
public class DeletionManager
    extends BasicLSMManager<MemTable, DeletionRequest, DeleteRequestContext> {

  // use wal manager object to write wal file on deletion
  private WALManager walManager;

  public DeletionManager(WALManager walManager) {
    this.walManager = walManager;
    initLevelProcess();
  }

  /**
   * write wal file on deletion
   *
   * @param root root memory node
   * @param context request context
   * @throws Exception
   */
  @Override
  public void preProcess(
      MemTable root, DeletionRequest deletionRequest, DeleteRequestContext context)
      throws Exception {
    walManager.write(deletionRequest);
  }

  @Override
  public void postProcess(MemTable root, DeletionRequest request, DeleteRequestContext context)
      throws Exception {}

  /** set the delete operation for each layer of memory nodes */
  private void initLevelProcess() {
    LevelProcessChain<MemTable, DeletionRequest, DeleteRequestContext> levelProcessChain =
        new LevelProcessChain<>();
    levelProcessChain
        .nextLevel(new MemTableDeletion())
        .nextLevel(new MemChunkGroupDeletion())
        .nextLevel(new MemChunkDeletion());
    setLevelProcessChain(levelProcessChain);
  }
}
