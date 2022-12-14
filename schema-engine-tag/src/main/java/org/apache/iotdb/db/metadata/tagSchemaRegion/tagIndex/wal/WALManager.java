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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.InsertionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTableGroup;
import org.apache.iotdb.lsm.request.IRequest;
import org.apache.iotdb.lsm.wal.IWALRecord;
import org.apache.iotdb.lsm.wal.WALReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Manage wal entry writes and reads */
public class WALManager extends org.apache.iotdb.lsm.manager.WALManager<MemTableGroup> {

  private static final Logger logger = LoggerFactory.getLogger(WALManager.class);

  private static final int INSERT = 1;

  private static final int DELETE = 2;

  public WALManager(
      String walDirPath,
      String walFilePrefix,
      int walBufferSize,
      IWALRecord walRecord,
      boolean forceEachWrite)
      throws IOException {
    super(walDirPath, walFilePrefix, walBufferSize, walRecord, forceEachWrite);
  }

  public WALManager(String walDirPath) {
    super(walDirPath);
  }

  /**
   * handle wal log writes for each request context
   *
   * @param request request context
   * @throws IOException
   */
  @Override
  public synchronized void write(MemTableGroup memTableGroup, IRequest request) {
    if (isRecover()) return;
    try {
      switch (request.getRequestType()) {
        case INSERT:
          process(memTableGroup, (InsertionRequest) request);
          break;
        case DELETE:
          process(memTableGroup, (DeletionRequest) request);
          break;
        default:
          break;
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * for recover
   *
   * @return request
   */
  @Override
  public synchronized IRequest read() {
    WALReader walReader = getWalReader();
    if (walReader.hasNext()) {
      WALEntry walEntry = (WALEntry) getWalReader().next();
      if (walEntry.getType() == INSERT) {
        return generateInsertRequest(walEntry);
      }
      if (walEntry.getType() == DELETE) {
        return generateDeleteRequest(walEntry);
      }
    }
    return null;
  }

  /**
   * generate insert context from wal entry
   *
   * @param walEntry wal entry
   * @return insert context
   */
  private InsertionRequest generateInsertRequest(WALEntry walEntry) {
    return new InsertionRequest(walEntry.getKeys(), walEntry.getDeviceID());
  }

  /**
   * generate delete context from wal entry
   *
   * @param walEntry wal entry
   * @return delete context
   */
  private DeletionRequest generateDeleteRequest(WALEntry walEntry) {
    return new DeletionRequest(walEntry.getKeys(), walEntry.getDeviceID());
  }

  /**
   * handle wal log writes for each insert context
   *
   * @param request insert request
   * @throws IOException
   */
  private void process(MemTableGroup memTableGroup, InsertionRequest request) throws IOException {
    WALEntry walEntry = new WALEntry(INSERT, request.getKeys(), request.getValue());
    if (checkUpdateWalFile(memTableGroup, request)) {
      updateFile(getWalFileName(currentFileID));
    }
    getWalWriter().write(walEntry);
  }

  private boolean checkUpdateWalFile(MemTableGroup memTableGroup, IRequest request) {
    if ((Integer) request.getValue() / memTableGroup.getNumOfDeviceIdsInMemTable()
        != currentFileID) {
      currentFileID = (Integer) request.getValue() / memTableGroup.getNumOfDeviceIdsInMemTable();
      return true;
    }
    return false;
  }

  /**
   * handle wal log writes for each delete context
   *
   * @param request delete context
   * @throws IOException
   */
  private void process(MemTableGroup memTableGroup, DeletionRequest request) throws IOException {
    WALEntry walEntry = new WALEntry(DELETE, request.getKeys(), request.getValue());
    if (checkUpdateWalFile(memTableGroup, request)) {
      updateFile(getWalFileName(currentFileID));
    }
    getWalWriter().write(walEntry);
  }
}
