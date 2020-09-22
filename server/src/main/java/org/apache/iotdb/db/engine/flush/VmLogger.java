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

package org.apache.iotdb.db.engine.flush;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

public class VmLogger {

  public static final String VM_LOG_NAME = ".vm.log";
  public static final String SOURCE_NAME = "source";
  public static final String TARGET_NAME = "target";
  public static final String MERGE_FINISHED = "merge finished";

  private static final String FORMAT_DEVICE_OFFSET = "%s %s";

  private BufferedWriter logStream;

  public VmLogger(String storageGroupDir, String tsfileName) throws IOException {
    logStream = new BufferedWriter(
        new FileWriter(SystemFileFactory.INSTANCE.getFile(storageGroupDir,
            tsfileName + VM_LOG_NAME), true));
  }

  public void close() throws IOException {
    logStream.close();
  }

  /**
   * We need to log the offset of the device instead of depending on the selfCheck() func to do the
   * truncation. Because if the current chunk group has been flushed to the tsfile but the device
   * hasn't been logged into vmLog then selfCheck() func won't truncate the chunk group. When we do
   * recovery, we will flush the device data again.
   */
  public void logDevice(String device, long offset) throws IOException {
    logStream.write(String.format(FORMAT_DEVICE_OFFSET, device, offset));
    logStream.newLine();
    logStream.flush();
  }

  public void logFile(String prefix, File file) throws IOException {
    logStream.write(prefix);
    logStream.newLine();
    logStream.write(file.getAbsolutePath());
    logStream.newLine();
    logStream.flush();
  }

  public void logMergeFinish() throws IOException {
    logStream.write(MERGE_FINISHED);
    logStream.newLine();
    logStream.flush();
  }

  public static boolean isVMLoggerFileExist(RestorableTsFileIOWriter writer) {
    File parent = writer.getFile().getParentFile();
    return FSFactoryProducer.getFSFactory()
        .getFile(parent, writer.getFile().getName() + VM_LOG_NAME).exists();
  }

}
