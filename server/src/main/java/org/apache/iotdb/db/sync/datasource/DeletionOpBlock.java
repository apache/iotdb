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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.sync.externalpipe.operation.DeleteOperation;
import org.apache.iotdb.db.sync.externalpipe.operation.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** This class covert 1 deletion action to 1 operation block. */
public class DeletionOpBlock extends AbstractOpBlock {
  private static final Logger logger = LoggerFactory.getLogger(DeletionOpBlock.class);

  private PartialPath deletePath; // This path can carry wildcard.
  // Deleted data rang is [startTime, endTime]
  private long startTime;
  private long endTime;

  public DeletionOpBlock(
      String sg, PartialPath deletePath, long startTime, long endTime, long pipeDataSerialNumber) {
    this(sg, deletePath, startTime, endTime, pipeDataSerialNumber, 0);
  }

  public DeletionOpBlock(
      String sg,
      PartialPath deletePath,
      long startTime,
      long endTime,
      long pipeDataSerialNumber,
      long beginIndex) {
    super(sg, pipeDataSerialNumber, beginIndex);
    this.deletePath = deletePath;
    this.startTime = startTime;
    this.endTime = endTime;

    this.dataCount = 1;
  }

  /**
   * Get 1 Operation that contain needed data.
   *
   * <p>Note:
   *
   * <p>1) Expected data range is [index, index+length)
   *
   * <p>2) Real returned data length can less than input parameter length. In fact, returned data's
   * range should always be [index, index+1)
   *
   * @param index
   * @param length
   * @return
   * @throws IOException
   */
  @Override
  public Operation getOperation(long index, long length) throws IOException {
    if (closed) {
      logger.error(
          "DeletionOpBlock.getOperation(), can not access closed DeletionOpBlock: {}.", this);
      throw new IOException("Can not access closed DeletionOpBlock: " + this);
    }

    if (index != beginIndex) {
      return null;
    }

    return new DeleteOperation(storageGroup, index, index + 1, deletePath, startTime, endTime);
  }

  /** release the current class object's resource */
  @Override
  public void close() {
    super.close();
  }

  @Override
  public String toString() {
    return super.toString()
        + ", deletePath="
        + deletePath
        + ", startTime="
        + startTime
        + ", endTime="
        + endTime;
  }
}
