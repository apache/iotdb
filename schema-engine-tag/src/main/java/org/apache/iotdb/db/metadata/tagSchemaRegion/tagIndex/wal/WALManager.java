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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaConfig;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.InsertionRequest;
import org.apache.iotdb.lsm.request.Request;
import org.apache.iotdb.lsm.wal.WALReader;
import org.apache.iotdb.lsm.wal.WALWriter;

import java.io.File;
import java.io.IOException;

/** Manage wal entry writes and reads */
public class WALManager {

  private static final String WAL_FILE_NAME = "tag_inverted_index.log";

  private static final int INSERT = 1;

  private static final int DELETE = 2;

  private final TagSchemaConfig tagSchemaConfig =
      TagSchemaDescriptor.getInstance().getTagSchemaConfig();

  private final String schemaDirPath;

  private File walFile;

  // directly use the wal writer that comes with the lsm framework
  private WALWriter walWriter;

  // directly use the wal reader that comes with the lsm framework
  private WALReader walReader;

  private boolean recover;

  public WALManager(String schemaDirPath) throws IOException {
    this.schemaDirPath = schemaDirPath;
    initFile(schemaDirPath);
    int walBufferSize = tagSchemaConfig.getWalBufferSize();
    walWriter = new WALWriter(walFile, walBufferSize, false);
    walReader = new WALReader(walFile, new WALEntry());
    recover = false;
  }

  private void initFile(String schemaDirPath) throws IOException {
    File schemaDir = new File(schemaDirPath);
    schemaDir.mkdirs();
    walFile = new File(this.schemaDirPath, WAL_FILE_NAME);
    if (!walFile.exists()) {
      walFile.createNewFile();
    }
  }

  /**
   * handle wal log writes for each request context
   *
   * @param request request context
   * @throws IOException
   */
  public synchronized void write(Request request) throws IOException {
    if (isRecover()) return;
    switch (request.getRequestType()) {
      case INSERT:
        process((InsertionRequest) request);
        break;
      case DELETE:
        process((DeletionRequest) request);
        break;
      default:
        break;
    }
  }

  /**
   * for recover
   *
   * @return request context
   */
  public synchronized Request read() {
    if (walReader.hasNext()) {
      WALEntry walEntry = (WALEntry) walReader.next();
      if (walEntry.getType() == INSERT) {
        return generateInsertRequest(walEntry);
      }
      if (walEntry.getType() == DELETE) {
        return generateDeleteContext(walEntry);
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
  private DeletionRequest generateDeleteContext(WALEntry walEntry) {
    return new DeletionRequest(walEntry.getKeys(), walEntry.getDeviceID());
  }

  /**
   * handle wal log writes for each insert context
   *
   * @param request insert request
   * @throws IOException
   */
  private void process(InsertionRequest request) throws IOException {
    WALEntry walEntry = new WALEntry(INSERT, request.getKeys(), request.getValue());
    walWriter.write(walEntry);
  }

  /**
   * handle wal log writes for each delete context
   *
   * @param request delete context
   * @throws IOException
   */
  private void process(DeletionRequest request) throws IOException {
    WALEntry walEntry = new WALEntry(DELETE, request.getKeys(), request.getValue());
    walWriter.write(walEntry);
  }

  @TestOnly
  public void close() throws IOException {
    walWriter.close();
    walReader.close();
  }

  public boolean isRecover() {
    return recover;
  }

  public void setRecover(boolean recover) {
    this.recover = recover;
  }
}
