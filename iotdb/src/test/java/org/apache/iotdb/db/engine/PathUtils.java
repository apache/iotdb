/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine;

import java.io.File;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;

public class PathUtils {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static Directories directories = Directories.getInstance();

  public static File getBufferWriteDir(String nameSpacePath) {
    String bufferwriteDirPath = directories.getFolderForTest();
    if (bufferwriteDirPath.length() > 0
        && bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1) != File.separatorChar) {
      bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
    }
    String dataDirPath = bufferwriteDirPath + nameSpacePath;
    File dataDir = new File(dataDirPath);
    return dataDir;
  }

  public static File getOverflowWriteDir(String nameSpacePath) {
    String overflowWriteDir = config.overflowDataDir;
    if (overflowWriteDir.length() > 0
        && overflowWriteDir.charAt(overflowWriteDir.length() - 1) != File.separatorChar) {
      overflowWriteDir = overflowWriteDir + File.separatorChar;
    }
    String dataDirPath = overflowWriteDir + nameSpacePath;
    File dataDir = new File(dataDirPath);
    return dataDir;
  }

  public static File getFileNodeDir(String nameSpacePath) {
    String filenodeDir = config.fileNodeDir;
    if (filenodeDir.length() > 0
        && filenodeDir.charAt(filenodeDir.length() - 1) != File.separatorChar) {
      filenodeDir = filenodeDir + File.separatorChar;
    }
    String dataDirPath = filenodeDir + nameSpacePath;
    File dataDir = new File(dataDirPath);
    return dataDir;
  }

}
