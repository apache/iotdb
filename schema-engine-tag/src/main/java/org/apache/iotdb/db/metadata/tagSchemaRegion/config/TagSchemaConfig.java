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
package org.apache.iotdb.db.metadata.tagSchemaRegion.config;

/** tag schema region config */
public class TagSchemaConfig {

  // the maximum number of device ids managed by a working memTable
  private int numOfDeviceIdsInMemTable = 65536;

  // the size of wal buffer used to store a wal record.(unit: byte)
  private int walBufferSize = 1024 * 1024;

  // the size of buffer used to write a record.(unit: byte)
  private int outBufferSize = 1024 * 1024;

  // degree of a b+ tree
  private int degree = 250;

  private int bPlusTreePageSize = 4 * 1024;

  // the maximum number of immutableMemTables, when this is reached, flush operation is required
  private int numOfImmutableMemTable = 5;

  // the size of max chunk in disk, if current chunk overflow, a new chunk is created to continue
  // store.(unit: byte)
  private long maxChunkSize = 16 * 1024;

  public int getNumOfDeviceIdsInMemTable() {
    return numOfDeviceIdsInMemTable;
  }

  public void setNumOfDeviceIdsInMemTable(int numOfDeviceIdsInMemTable) {
    this.numOfDeviceIdsInMemTable = numOfDeviceIdsInMemTable;
  }

  public int getNumOfImmutableMemTable() {
    return numOfImmutableMemTable;
  }

  public void setNumOfImmutableMemTable(int numOfImmutableMemTable) {
    this.numOfImmutableMemTable = numOfImmutableMemTable;
  }

  public int getWalBufferSize() {
    return walBufferSize;
  }

  public void setWalBufferSize(int walBufferSize) {
    this.walBufferSize = walBufferSize;
  }

  public int getOutBufferSize() {
    return outBufferSize;
  }

  public void setOutBufferSize(int outBufferSize) {
    this.outBufferSize = outBufferSize;
  }

  public int getDegree() {
    return degree;
  }

  public void setDegree(int degree) {
    this.degree = degree;
  }

  public int getBPlusTreePageSize() {
    return bPlusTreePageSize;
  }

  public void setBPlusTreePageSize(int bPlusTreePageSize) {
    this.bPlusTreePageSize = bPlusTreePageSize;
  }

  public long getMaxChunkSize() {
    return maxChunkSize;
  }

  public void setMaxChunkSize(long maxChunkSize) {
    this.maxChunkSize = maxChunkSize;
  }

  @Override
  public String toString() {
    return "TagSchemaConfig{"
        + "numOfDeviceIdsInMemTable="
        + numOfDeviceIdsInMemTable
        + ", walBufferSize="
        + walBufferSize
        + ", outBufferSize="
        + outBufferSize
        + ", degree="
        + degree
        + ", numOfImmutableMemTable="
        + numOfImmutableMemTable
        + ", maxChunkSize="
        + maxChunkSize
        + '}';
  }
}
