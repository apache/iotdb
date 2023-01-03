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
package org.apache.iotdb.lsm.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/** write records to wal file */
public class WALWriter implements IWALWriter {
  private static final Logger logger = LoggerFactory.getLogger(WALWriter.class);
  // wal file
  private File logFile;
  private DataOutputStream outputStream;

  public WALWriter(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    outputStream =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(logFile, true)));
  }

  /**
   * write walRecord to wal file
   *
   * @param walRecord record to be written
   * @throws IOException
   */
  @Override
  public void write(IWALRecord walRecord) throws IOException {
    walRecord.serialize(outputStream);
  }

  public void update(File logFile) throws IOException {
    close();
    this.logFile = logFile;
    outputStream = new DataOutputStream(new FileOutputStream(logFile, true));
  }

  @Override
  public void close() throws IOException {
    if (outputStream != null) {
      try {
        outputStream.flush();
      } finally {
        outputStream.close();
      }
    }
  }

  @Override
  public String toString() {
    return "WALLogWriter{" + "logFile=" + logFile + '}';
  }
}
