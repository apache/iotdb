/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.logfile;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/** reader for reading mlog.txt */
public class MLogTxtReader implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MLogTxtReader.class);

  private BufferedReader bufferedReader;
  private File logFile;
  private String cmd;

  public MLogTxtReader(String schemaDir, String logFileName) throws IOException {
    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      logger.error("no mlog.txt to init MManager.");
      throw new IOException("mlog.txt does not exist.");
    }

    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    bufferedReader = new BufferedReader(new FileReader(logFile));
  }

  public String next() {
    String ret = cmd;
    cmd = null;
    return ret;
  }

  public boolean hasNext() {
    if (cmd != null) {
      return true;
    }
    try {
      return (cmd = bufferedReader.readLine()) != null;
    } catch (IOException e) {
      logger.warn("Read mlog error.");
      cmd = null;
      return false;
    }
  }

  @Override
  public void close() {
    try {
      bufferedReader.close();
    } catch (IOException e) {
      logger.error("Failed to close mlog.txt");
    }
  }
}
