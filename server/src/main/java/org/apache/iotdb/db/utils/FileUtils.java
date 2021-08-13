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
package org.apache.iotdb.db.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;

public class FileUtils {
  private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {}

  public static void deleteDirectory(File folder) {
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        deleteDirectory(file);
      }
    }
    try {
      Files.delete(folder.toPath());
    } catch (NoSuchFileException | DirectoryNotEmptyException e) {
      logger.warn("{}: {}", e.getMessage(), Arrays.toString(folder.list()), e);
    } catch (Exception e) {
      logger.warn("{}: {}", e.getMessage(), folder.getName(), e);
    }
  }
}
