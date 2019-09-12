/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.fileSystem;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.write.writer.DefaultTsFileOutput;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;

public enum FileOutputFactory {

  INSTANCE;

  private static FSType fsType = TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);

  public TsFileOutput getTsFileOutput(String filePath, boolean append) {
    try {
      if (fsType.equals(FSType.HDFS)) {
        return new HDFSOutput(filePath, append);
      } else {
        return new DefaultTsFileOutput(new FileOutputStream(filePath, append));
      }
    } catch (IOException e) {
      logger.error("Failed to get TsFile Output: ", e);
      return null;
    }
  }
}
