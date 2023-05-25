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
package org.apache.iotdb.db.wal.utils;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.wal.exception.MemTablePinException;
import org.apache.iotdb.db.wal.exception.WALPipeException;
import org.apache.iotdb.db.wal.node.WALNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This handler is used by the Pipe to find the corresponding insert node. Besides, it can try to
 * pin/unpin the wal entries by the memTable id.
 */
public class WALPipeHandler {
  private static final Logger logger = LoggerFactory.getLogger(WALPipeHandler.class);

  private long memTableId = -1;
  /** cached value, null after this value is flushed to wal successfully */
  private volatile WALEntryValue value;
  /** wal entry's position in the wal, valid after the value is flushed to wal successfully */
  private final WALEntryPosition walEntryPosition = new WALEntryPosition();
  /** wal node, null when wal is disabled */
  private WALNode walNode = null;

  public WALPipeHandler(WALEntryValue value) {
    this.value = value;
  }

  /**
   * Pin the wal files of the given memory table. Notice: cannot pin one memTable too long,
   * otherwise the wal disk usage may too large.
   *
   * @throws MemTablePinException If the memTable has been flushed
   */
  public void pinMemTable() throws MemTablePinException {
    if (walNode == null || memTableId < 0) {
      throw new MemTablePinException("Fail to pin memTable because of internal error.");
    }
    walNode.pinMemTable(memTableId);
  }

  /**
   * Unpin the wal files of the given memory table.
   *
   * @throws MemTablePinException If there aren't corresponding pin operations
   */
  public void unpinMemTable() throws MemTablePinException {
    if (walNode == null || memTableId < 0) {
      throw new MemTablePinException("Fail to pin memTable because of internal error.");
    }
    walNode.unpinMemTable(memTableId);
  }

  /** Get this handler's value */
  public InsertNode getValue() throws WALPipeException {
    // return local cache
    WALEntryValue res = value;
    if (res != null) {
      if (res instanceof InsertNode) {
        return (InsertNode) value;
      } else {
        throw new WALPipeException("Fail to get value because the entry type isn't InsertNode.");
      }
    }
    // wait until the position is ready
    while (!walEntryPosition.canRead()) {
      try {
        synchronized (this) {
          this.wait();
        }
      } catch (InterruptedException e) {
        logger.warn("Interrupted when waiting for result.", e);
        Thread.currentThread().interrupt();
      }
    }
    // read from the wal file
    try {
      return walEntryPosition.readInsertNodeViaCache();
    } catch (Exception e) {
      throw new WALPipeException("Fail to get value because the file content isn't correct.", e);
    }
  }

  public long getMemTableId() {
    return memTableId;
  }

  public void setMemTableId(long memTableId) {
    this.memTableId = memTableId;
  }

  public void setWalNode(WALNode walNode) {
    this.walNode = walNode;
    this.walEntryPosition.setWalNode(walNode);
  }

  public WALEntryPosition getWalEntryPosition() {
    return walEntryPosition;
  }

  public void setEntryPosition(long walFileVersionId, long position) {
    this.walEntryPosition.setEntryPosition(walFileVersionId, position);
    this.value = null;
    synchronized (this) {
      this.notifyAll();
    }
  }

  public int getSize() {
    return walEntryPosition.getSize();
  }

  public void setSize(int size) {
    this.walEntryPosition.setSize(size);
  }

  @Override
  public String toString() {
    return "WALPipeHandler{"
        + "memTableId="
        + memTableId
        + ", value="
        + value
        + ", walEntryPosition="
        + walEntryPosition
        + ", walNode="
        + walNode
        + '}';
  }
}
