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
package org.apache.iotdb.db.engine.archive;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.logfile.MLogTxtWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * ArchiveOperateWriter writes the binary logs of ArchiveOperate into file using FileOutputStream
 */
public class ArchiveOperateWriter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MLogTxtWriter.class);
  private final File logFile;
  private FileOutputStream logFileOutStream;

  public ArchiveOperateWriter(String logFileName) throws FileNotFoundException {
    this(SystemFileFactory.INSTANCE.getFile(logFileName));
  }

  public ArchiveOperateWriter(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    if (!logFile.exists()) {
      if (logFile.getParentFile() != null) {
        if (logFile.getParentFile().mkdirs()) {
          logger.info("created archive log folder");
        } else {
          logger.info("create archive log folder failed");
        }
      }
    }
    logFileOutStream = new FileOutputStream(logFile, true);
  }

  public void log(ArchiveOperate.ArchiveOperateType type, ArchiveTask task) throws IOException {
    ArchiveOperate operate;
    switch (type) {
      case SET:
        operate = new ArchiveOperate(ArchiveOperate.ArchiveOperateType.SET, task);
        operate.serialize(logFileOutStream);
        break;
      case CANCEL:
      case START:
      case PAUSE:
      case RESUME:
      case FINISHED:
      case ERROR:
        operate = new ArchiveOperate(type, task.getTaskId());
        operate.serialize(logFileOutStream);
        break;
    }
  }

  @Override
  public void close() throws Exception {
    logFileOutStream.close();
  }
}
