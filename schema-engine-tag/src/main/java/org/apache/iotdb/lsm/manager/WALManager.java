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
package org.apache.iotdb.lsm.manager;

import org.apache.iotdb.lsm.request.Request;
import org.apache.iotdb.lsm.wal.IWALRecord;
import org.apache.iotdb.lsm.wal.WALReader;
import org.apache.iotdb.lsm.wal.WALWriter;

import java.io.File;
import java.io.IOException;

/** Manage wal entry writes and reads */
public abstract class WALManager {

  private final String schemaDirPath;

  private File walFile;

  // directly use the wal writer that comes with the lsm framework
  private WALWriter walWriter;

  // directly use the wal reader that comes with the lsm framework
  private WALReader walReader;

  private boolean recover;

  public WALManager(String schemaDirPath) {
    this.schemaDirPath = schemaDirPath;
  }

  public WALManager(
      String schemaDirPath,
      String walFileName,
      int walBufferSize,
      IWALRecord walRecord,
      boolean forceEachWrite)
      throws IOException {
    this.schemaDirPath = schemaDirPath;
    initFile(schemaDirPath, walFileName);
    walWriter = new WALWriter(walFile, walBufferSize, forceEachWrite);
    walReader = new WALReader(walFile, walRecord);
    recover = false;
  }

  private void initFile(String schemaDirPath, String walFileName) throws IOException {
    File schemaDir = new File(schemaDirPath);
    schemaDir.mkdirs();
    walFile = new File(this.schemaDirPath, walFileName);
    if (!walFile.exists()) {
      walFile.createNewFile();
    }
  }

  /**
   * handle wal log writes for each request
   *
   * @param request request context
   * @throws IOException
   */
  public abstract void write(Request request) throws IOException;

  /**
   * for recover
   *
   * @return request
   */
  public abstract Request read();

  public void close() throws IOException {
    walWriter.close();
    walReader.close();
  }

  public String getSchemaDirPath() {
    return schemaDirPath;
  }

  public File getWalFile() {
    return walFile;
  }

  public void setWalFile(File walFile) {
    this.walFile = walFile;
  }

  public WALWriter getWalWriter() {
    return walWriter;
  }

  public void setWalWriter(WALWriter walWriter) {
    this.walWriter = walWriter;
  }

  public WALReader getWalReader() {
    return walReader;
  }

  public void setWalReader(WALReader walReader) {
    this.walReader = walReader;
  }

  public boolean isRecover() {
    return recover;
  }

  public void setRecover(boolean recover) {
    this.recover = recover;
  }
}
