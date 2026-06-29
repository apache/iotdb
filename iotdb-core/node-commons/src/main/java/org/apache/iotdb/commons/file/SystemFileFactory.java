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

package org.apache.iotdb.commons.file;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.fileSystem.FSType;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public enum SystemFileFactory {
  INSTANCE;

  private static FSType fsType =
      CommonDescriptor.getInstance().getConfig().getSystemFileStorageFs();
  private static final String UNSUPPORTED_FILE_SYSTEM = "Unsupported file system: ";

  public File getFile(String pathname) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORTED_FILE_SYSTEM + fsType.name());
    } else {
      return new File(pathname);
    }
  }

  public File getFile(String parent, String child) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORTED_FILE_SYSTEM + fsType.name());
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(File parent, String child) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORTED_FILE_SYSTEM + fsType.name());
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(URI uri) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORTED_FILE_SYSTEM + fsType.name());
    } else {
      return new File(uri);
    }
  }

  public void makeDirIfNecessary(String dir) throws IOException {
    File directory = getFile(dir);
    if (directory.isFile()) {
      directory = directory.getParentFile();
    }
    if (directory.exists()) {
      return;
    }
    FileUtils.forceMkdir(directory);
  }
}
