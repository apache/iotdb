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

package org.apache.iotdb.cluster.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;

@SuppressWarnings("java:S1135")
public class IOUtils {

  private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

  private IOUtils() {
    // util class
  }

  /**
   * An interface that is used for a node to pull chunks of files like TsFiles. The file should be a
   * temporary hard link, and once the file is totally read, it will be removed.
   */
  public static ByteBuffer readFile(String filePath, long offset, int length) throws IOException {
    // TODO-Cluster: hold if the file is an unclosed TsFile
    File file = new File(filePath);
    if (!file.exists()) {
      return ByteBuffer.allocate(0);
    }

    ByteBuffer result;
    boolean fileExhausted;
    try (BufferedInputStream bufferedInputStream =
        new BufferedInputStream(new FileInputStream(file))) {
      skipExactly(bufferedInputStream, offset);
      byte[] bytes = new byte[length];
      result = ByteBuffer.wrap(bytes);
      int len = bufferedInputStream.read(bytes);
      result.limit(Math.max(len, 0));
      fileExhausted = bufferedInputStream.available() <= 0;
    }

    if (fileExhausted) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete an exhausted file {}", filePath, e);
      }
    }
    return result;
  }

  private static void skipExactly(InputStream stream, long byteToSkip) throws IOException {
    while (byteToSkip > 0) {
      byteToSkip -= stream.skip(byteToSkip);
    }
  }

  public static Throwable getRootCause(Throwable e) {
    Throwable curr = e;
    while (curr.getCause() != null) {
      curr = curr.getCause();
    }
    return curr;
  }
}
