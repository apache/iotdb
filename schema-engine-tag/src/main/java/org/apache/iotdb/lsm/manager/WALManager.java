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

import org.apache.iotdb.lsm.request.IRequest;
import org.apache.iotdb.lsm.wal.IWALRecord;
import org.apache.iotdb.lsm.wal.WALReader;
import org.apache.iotdb.lsm.wal.WALWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/** Manage wal entry writes and reads */
public abstract class WALManager<T> {

  private final String walDirPath;

  private File walFile;

  // directly use the wal writer that comes with the lsm framework
  private WALWriter walWriter;

  // directly use the wal reader that comes with the lsm framework
  private WALReader walReader;

  private String[] walFileNames;

  private int currentFileIndex = 0;

  protected int currentFileID = 0;

  protected String walFilePrefix;

  private boolean recover;

  private IWALRecord walRecord;

  public WALManager(String walDirPath) {
    this.walDirPath = walDirPath;
  }

  public WALManager(String walDirPath, String walFilePrefix, IWALRecord walRecord) {
    this.walDirPath = walDirPath;
    this.walFilePrefix = walFilePrefix;
    this.walRecord = walRecord;
    recover = false;
  }

  public void initRecover() throws IOException {
    File walDir = new File(walDirPath);
    walDir.mkdirs();
    File[] walFiles = walDir.listFiles();
    walFileNames =
        Arrays.stream(walFiles)
            .map(this::getWalFileID)
            .sorted()
            .map(this::getWalFileName)
            .toArray(String[]::new);
    String walFileName;
    if (walFileNames.length == 0) {
      walFileName = getWalFileName(currentFileID);
    } else {
      walFileName = walFileNames[currentFileIndex];
    }
    initWalWriterAndReader(initFile(walFileName));
  }

  public IRequest recover() throws IOException {
    IRequest request = this.read();
    while (request == null && currentFileIndex < walFileNames.length - 1) {
      currentFileIndex++;
      updateFile(walFileNames[currentFileIndex]);
      request = this.read();
    }
    return request;
  }

  public void deleteWalFile(int index) {
    File file = new File(this.walDirPath, getWalFileName(index));
    file.delete();
  }

  protected Integer getWalFileID(File file) {
    return Integer.parseInt(file.getName().substring(walFilePrefix.length()));
  }

  protected String getWalFileName(Integer ID) {
    return walFilePrefix + ID;
  }

  private File initFile(String walFileName) throws IOException {
    walFile = new File(this.walDirPath, walFileName);
    if (!walFile.exists()) {
      walFile.createNewFile();
    }
    return walFile;
  }

  private void initWalWriterAndReader(File walFile) throws IOException {
    walWriter = new WALWriter(walFile);
    walReader = new WALReader(walFile, this.walRecord);
  }

  public void updateFile(String walFileName) throws IOException {
    initFile(walFileName);
    walWriter.update(walFile);
    walReader.update(walFile);
  }

  /**
   * handle wal log writes for each request
   *
   * @param request request context
   * @throws IOException
   */
  public abstract void write(T root, IRequest request);

  /**
   * for recover, read a wal record and generate it as a request
   *
   * @return request
   */
  public abstract IRequest read();

  public void close() throws IOException {
    walWriter.close();
    walReader.close();
  }

  public String getSchemaDirPath() {
    return walDirPath;
  }

  public File getWalFile(int index) {
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

  public String getWalDirPath() {
    return walDirPath;
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
