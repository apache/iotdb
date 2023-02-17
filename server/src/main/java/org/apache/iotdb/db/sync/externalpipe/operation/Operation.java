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

/** Operation represents the data changes of the server. */
public abstract class Operation {
  public enum OperationType {
    INSERT,
    DELETE;
  }

  private OperationType operationType;

  private final String storageGroup;
  // The data rang is [startIndex, endIndex),
  // i.e. valid data include startIndex and not include endIndex
  private long startIndex;
  private long endIndex;

  protected Operation(
      OperationType operationType, String storageGroup, long startIndex, long endIndex) {
    this.operationType = operationType;
    this.storageGroup = storageGroup;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public String getOperationTypeName() {
    return operationType.name();
  }

  public long getStartIndex() {
    return startIndex;
  }

  public long getEndIndex() {
    return endIndex;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public long getDataCount() {
    return endIndex - startIndex;
  }

  @Override
  public String toString() {
    return "operationType="
        + getOperationTypeName()
        + ", storageGroup="
        + storageGroup
        + ", startIndex="
        + startIndex
        + ", endIndex="
        + endIndex;
  }
}
