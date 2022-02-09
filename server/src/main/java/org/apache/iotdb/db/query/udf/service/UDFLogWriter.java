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

package org.apache.iotdb.db.query.udf.service;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class UDFLogWriter {

  public static final Byte REGISTER_TYPE = 0;
  public static final Byte DEREGISTER_TYPE = 1;

  private static final String REGISTER_TYPE_STRING = REGISTER_TYPE.toString();
  private static final String DEREGISTER_TYPE_STRING = DEREGISTER_TYPE.toString();

  private final File logFile;
  private final BufferedWriter writer;

  public UDFLogWriter(String logFileName) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
    FileWriter fileWriter = new FileWriter(logFile, true);
    writer = new BufferedWriter(fileWriter);
  }

  public void close() throws IOException {
    writer.close();
  }

  public void deleteLogFile() throws IOException {
    if (!logFile.delete()) {
      throw new IOException("Failed to delete " + logFile + ".");
    }
  }

  public void register(String functionName, String className) throws IOException {
    writer.write(String.format("%s,%s,%s", REGISTER_TYPE_STRING, functionName, className));
    writeLineAndFlush();
  }

  public void deregister(String functionName) throws IOException {
    writer.write(String.format("%s,%s", DEREGISTER_TYPE_STRING, functionName));
    writeLineAndFlush();
  }

  private void writeLineAndFlush() throws IOException {
    writer.newLine();
    writer.flush();
  }
}
