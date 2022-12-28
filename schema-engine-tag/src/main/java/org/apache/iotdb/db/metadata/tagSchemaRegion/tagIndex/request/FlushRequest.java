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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.request.IFlushRequest;

public class FlushRequest implements IFlushRequest<String, String, MemTable> {
  private String flushDirPath;
  private String flushFileName;

  private String flushDeleteFileName;

  private long chunkMaxSize;

  private int index;
  private MemTable memNode;

  public MemTable getMemNode() {
    return memNode;
  }

  public void setMemNode(MemTable memNode) {
    this.memNode = memNode;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public FlushRequest(int index, MemTable memNode, long chunkMaxSize) {
    this.index = index;
    this.memNode = memNode;
    this.chunkMaxSize = chunkMaxSize;
  }

  public String getFlushDirPath() {
    return flushDirPath;
  }

  public void setFlushDirPath(String flushDirPath) {
    this.flushDirPath = flushDirPath;
  }

  public String getFlushFileName() {
    return flushFileName;
  }

  public void setFlushFileName(String flushFileName) {
    this.flushFileName = flushFileName;
  }

  public long getChunkMaxSize() {
    return chunkMaxSize;
  }

  public void setChunkMaxSize(long chunkMaxSize) {
    this.chunkMaxSize = chunkMaxSize;
  }

  public String getFlushDeletionFileName() {
    return flushDeleteFileName;
  }

  public void setFlushDeleteFileName(String flushDeleteFileName) {
    this.flushDeleteFileName = flushDeleteFileName;
  }
}
