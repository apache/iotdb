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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.lsm.context.InsertContext;
import org.apache.iotdb.lsm.levelProcess.InsertLevelProcess;

import java.util.List;

public class MemChunkInsertion extends InsertLevelProcess<MemChunk, Object> {
  @Override
  public List<Object> getChildren(MemChunk memNode, InsertContext context) {
    return null;
  }

  @Override
  public void insert(MemChunk memNode, InsertContext context) {
    Integer deviceID = (Integer) context.getValue();
    memNode.put(deviceID);
  }
}
