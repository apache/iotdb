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
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.SingleFileLogReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MLogReader implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MLogReader.class);
  private File logFile;

  SingleFileLogReader singleFileLogReader;

  public MLogReader(String schemaDir, String logFileName) throws IOException {
    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      logger.error("no mlog.bin to init MManager.");
      throw new IOException("mlog.bin does not exist.");
    }

    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    singleFileLogReader = new SingleFileLogReader(logFile);
  }

  public MLogReader(String logFilePath) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    singleFileLogReader = new SingleFileLogReader(logFile);
  }

  public MLogReader(File logFile) throws IOException {
    this.logFile = logFile;
    singleFileLogReader = new SingleFileLogReader(this.logFile);
  }

  public boolean hasNext() {
    return !singleFileLogReader.isFileCorrupted() && singleFileLogReader.hasNext();
  }

  public PhysicalPlan next() {
    return singleFileLogReader.next();
  }

  @Override
  public void close() {
    singleFileLogReader.close();
  }

  public boolean isFileCorrupted() {
    return singleFileLogReader.isFileCorrupted();
  }
}
