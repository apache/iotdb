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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class FSUtils {
  private static final Logger logger = LoggerFactory.getLogger(FSUtils.class);
  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final FSType[] fsTypes = {FSType.OBJECT_STORAGE, FSType.HDFS};
  public static final int PATH_FROM_SEQUENCE_LEVEL = 5;
  public static final int PATH_FROM_DATABASE_LEVEL = 4;
  public static final String[] fsPrefix = {"os://", "hdfs://"};
  public static final String OS_FILE_SEPARATOR = "/";
  private static final String[] fsFileClassName = {
    config.getObjectStorageFile(), config.getHdfsFile()
  };

  private static final boolean[] isSupported = new boolean[fsTypes.length];
  private static final Class<?>[] fsFileClass = new Class[fsTypes.length];

  private FSUtils() {}

  static {
    reload();
  }

  public static synchronized void reload() {
    fsFileClassName[0] = config.getObjectStorageFile();
    fsFileClassName[1] = config.getHdfsFile();
    for (FSType fsType : config.getTSFileStorageFs()) {
      if (fsType == FSType.OBJECT_STORAGE) {
        isSupported[0] = true;
      } else if (fsType == FSType.HDFS) {
        isSupported[1] = true;
      }
    }

    for (int i = 0; i < fsTypes.length; ++i) {
      if (!isSupported[i]) {
        continue;
      }
      try {
        fsFileClass[i] = Class.forName(fsFileClassName[i]);
      } catch (ClassNotFoundException e) {
        logger.error(
            "Failed to get "
                + fsTypes[i].name()
                + " file system. Please check your dependency of "
                + fsTypes[i].name()
                + " module.",
            e);
      }
    }
  }

  public static FSType getFSType(File file) {
    for (int i = 0; i < fsTypes.length; ++i) {
      if (fsFileClass[i] != null && fsFileClass[i].isInstance(file)) {
        return fsTypes[i];
      }
    }
    return FSType.LOCAL;
  }

  public static FSType getFSType(String fsPath) {
    for (int i = 0; i < fsTypes.length; ++i) {
      if (fsPath.startsWith(fsPrefix[i])) {
        return fsTypes[i];
      }
    }
    return FSType.LOCAL;
  }

  public static String getPath(String fsPath) {
    for (int i = 0; i < fsTypes.length; ++i) {
      if (fsPath.startsWith(fsPrefix[i])) {
        return fsPath.substring(fsPrefix[i].length());
      }
    }
    return fsPath;
  }

  public static FSPath parse(String fsPath) {
    FSType type = FSType.LOCAL;
    String path = fsPath;
    for (int i = 0; i < fsTypes.length; ++i) {
      if (fsPath.startsWith(fsPrefix[i])) {
        type = fsTypes[i];
        break;
      }
    }
    return new FSPath(type, path);
  }

  public static String getOSDefaultPath(String bucket, int dataNodeId) {
    return new FSPath(FSType.OBJECT_STORAGE, fsPrefix[0] + bucket + OS_FILE_SEPARATOR + dataNodeId)
        .getPath();
  }

  public static FSPath parseLocalTsFile2OSFile(File localFile, String bucket, int dataNodeId)
      throws IOException {
    return new FSPath(
        FSType.OBJECT_STORAGE,
        fsPrefix[0]
            + bucket
            + OS_FILE_SEPARATOR
            + dataNodeId
            + OS_FILE_SEPARATOR
            + getLocalTsFileShortPath(localFile, PATH_FROM_SEQUENCE_LEVEL));
  }

  public static String getLocalTsFileShortPath(File localTsFile, int level) throws IOException {
    String[] filePathSplits = FilePathUtils.splitTsFilePath(localTsFile.getCanonicalPath());
    return String.join(
        OS_FILE_SEPARATOR,
        Arrays.copyOfRange(filePathSplits, filePathSplits.length - level, filePathSplits.length));
  }

  public static boolean isLocal(String fsPath) {
    return getFSType(fsPath) == FSType.LOCAL;
  }

  public static boolean isLocal(File file) {
    return getFSType(file) == FSType.LOCAL;
  }
}
