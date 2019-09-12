/**
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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.fileSystem.TSFileFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CommonUtils {

  private CommonUtils(){}

  /**
   * get JDK version.
   *
   * @return JDK version (int type)
   */
  public static int getJdkVersion() {
    String[] javaVersionElements = System.getProperty("java.version").split("\\.");
    if (Integer.parseInt(javaVersionElements[0]) == 1) {
      return Integer.parseInt(javaVersionElements[1]);
    } else {
      return Integer.parseInt(javaVersionElements[0]);
    }
  }

  /**
   * NOTICE: This method is currently used only for data dir, thus using TSFileFactory to get file
   *
   * @param dir directory path
   * @return
   */
  public static long getUsableSpace(String dir) {
    return TSFileFactory.INSTANCE.getFile(dir).getFreeSpace();
  }

  public static boolean hasSpace(String dir) {
    return getUsableSpace(dir) > 0;
  }

  public static long getOccupiedSpace(String folderPath) throws IOException {
    Path folder = Paths.get(folderPath);
    return Files.walk(folder).filter(p -> p.toFile().isFile())
        .mapToLong(p -> p.toFile().length()).sum();
  }
}
