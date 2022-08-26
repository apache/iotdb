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

package org.apache.iotdb.db.sync.externalpipe.operation;

import org.apache.iotdb.commons.path.PartialPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 1 DeleteOperation contain 1 deletion info. */
public class DeleteOperation extends Operation {
  private static final Logger logger = LoggerFactory.getLogger(DeleteOperation.class);

  private PartialPath deletePath; // This path can contain wildcard.
  // Deleted data ran is [startTime, endTime]
  private long startTime;
  private long endTime;

  public DeleteOperation(
      String storageGroup,
      long startIndex,
      long endIndex,
      PartialPath deletePath,
      long startTime,
      long endTime) {
    super(OperationType.DELETE, storageGroup, startIndex, endIndex);
    this.deletePath = deletePath;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public PartialPath getDeletePath() {
    return deletePath;
  }

  public String getDeletePathStr() {
    return deletePath.getFullPath();
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  @Override
  public String toString() {

    return "DeleteOperation{"
        + super.toString()
        + ", deletePath="
        + deletePath.getFullPath()
        + ", startTime="
        + startTime
        + ", endTime="
        + endTime
        + '}';
  }
}
