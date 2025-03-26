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

package org.apache.iotdb.metrics.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileStoreUtils {
  private static final Logger logger = LoggerFactory.getLogger(FileStoreUtils.class);

  // get the FileStore of dir .if current dir is not exists, check parent dir.
  // for example, the dn_wal_dirs default value is data/datanode/wal and system will save the
  // data in the relative path directory it indicates under the IoTDB folder.
  // it will check the parent dir until find the existing dir.
  // if the parent dir = null(when path =  data in this example), it will use the IoTDB folder.
  public static FileStore getFileStore(String dir) {
    Path path = Paths.get(dir);
    FileStore fileStore = null;

    while (fileStore == null && path != null) {
      try {
        fileStore = Files.getFileStore(path);
      } catch (NoSuchFileException e) {
        path = path.getParent();
      } catch (IOException e) {
        logger.warn("Failed to get storage path of {}, because", dir, e);
        break;
      }
    }
    // If the dir is a relative dir, final use the IoTDB folder.
    if (path == null) {
      path = Paths.get("./");
      try {
        fileStore = Files.getFileStore(path);
      } catch (IOException innerException) {
        logger.warn("Failed to get storage path of {}, because", dir, innerException);
      }
    }
    return fileStore;
  }

  public static void main(String[] args) {
     printFile("/Applications");
     printFile("/Users");
     printFile("/Users/root");
  }

  public static void printFile(String path) {
    try {
      // 获取文件存储对象（FileStore）
      FileStore fileStore = FileStoreUtils.getFileStore(path);

      // 输出文件所在磁盘的详细信息
      System.out.println("文件所在的磁盘: " + fileStore);
      System.out.println("文件系统类型: " + fileStore.type());
      System.out.println("可用空间: " + fileStore.getUsableSpace());
      System.out.println("总空间: " + fileStore.getTotalSpace());
      System.out.println("是否只读: " + fileStore.isReadOnly());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
