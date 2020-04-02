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

package org.apache.iotdb.tsfile.fileSystem.fsFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFSFactory implements FSFactory {

  private static final Logger logger = LoggerFactory.getLogger(LocalFSFactory.class);

  @Override
  public File getFileWithParent(String pathname) {
    File res = new File(pathname);
    if (!res.exists()) {
      res.getParentFile().mkdirs();
    }
    return res;
  }

  public File getFile(String pathname) {
    return new File(pathname);
  }

  public File getFile(String parent, String child) {
    return new File(parent, child);
  }

  public File getFile(File parent, String child) {
    return new File(parent, child);
  }

  public File getFile(URI uri) {

    return new File(uri);
  }

  public BufferedReader getBufferedReader(String filePath) {
    try {
      return new BufferedReader(new FileReader(filePath));
    } catch (IOException e) {
      logger.error("Failed to get buffered reader for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    try {
      return new BufferedWriter(new FileWriter(filePath, append));
    } catch (IOException e) {
      logger.error("Failed to get buffered writer for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedInputStream getBufferedInputStream(String filePath) {
    try {
      return new BufferedInputStream(new FileInputStream(filePath));
    } catch (IOException e) {
      logger.error("Failed to get buffered input stream for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    try {
      return new BufferedOutputStream(new FileOutputStream(filePath));
    } catch (IOException e) {
      logger.error("Failed to get buffered output stream for {}. ", filePath, e);
      return null;
    }
  }

  public void moveFile(File srcFile, File destFile) {
    try {
      FileUtils.moveFile(srcFile, destFile);
    } catch (IOException e) {
      logger.error("Failed to move file from {} to {}. ", srcFile.getAbsolutePath(),
          destFile.getAbsolutePath(), e);
    }
  }

  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    return new File(fileFolder).listFiles(file -> file.getName().endsWith(suffix));
  }

  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    return new File(fileFolder).listFiles(file -> file.getName().startsWith(prefix));
  }
}