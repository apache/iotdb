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
 *
 */

package org.apache.iotdb.db.sync.datasource;

import org.apache.iotdb.db.sync.externalpipe.operation.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** This abstract class is used to manage PIPE operation block */
public abstract class AbstractOpBlock implements Comparable<AbstractOpBlock> {
  private static final Logger logger = LoggerFactory.getLogger(AbstractOpBlock.class);

  // StorageGroup Name
  protected String storageGroup;
  long filePipeSerialNumber;

  boolean closed = false;

  // record First Entry's index
  protected long beginIndex = -1;
  // data number of this data file
  protected long dataCount = -1;

  public AbstractOpBlock(String storageGroupName) {
    this(storageGroupName, 0);
  }

  public AbstractOpBlock(String storageGroupName, long beginIndex) {
    this.storageGroup = storageGroupName;
    this.beginIndex = beginIndex;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public long getDataCount() {
    return dataCount;
  }

  public void setBeginIndex(long beginIndex) {
    this.beginIndex = beginIndex;
  }

  public long getBeginIndex() {
    return beginIndex;
  }

  /**
   * return the BeginIndex of next SrcEntry
   *
   * @return
   */
  public long getNextIndex() {
    return beginIndex + dataCount;
  }

  @Override
  public int compareTo(AbstractOpBlock o) {
    return beginIndex > o.beginIndex ? 1 : (beginIndex == o.beginIndex ? 0 : -1);
  }

  /**
   * Get data from data src
   *
   * @param index
   * @param length
   * @return
   * @throws IOException
   */
  public abstract Operation getOperation(long index, long length) throws IOException;

  /** release current class' resource */
  public void close() {
    closed = true;
  };

  public boolean isClosed() {
    return closed;
  };

  public long getFilePipeSerialNumber() {
    return filePipeSerialNumber;
  }

  public void setFilePipeSerialNumber(long filePipeSerialNumber) {
    this.filePipeSerialNumber = filePipeSerialNumber;
  }

  @Override
  public String toString() {
    return "storageGroup="
        + storageGroup
        + ", filePipeSerialNumber="
        + filePipeSerialNumber
        + ", beginIndex="
        + beginIndex
        + ", dataCount="
        + dataCount;
  }
}
