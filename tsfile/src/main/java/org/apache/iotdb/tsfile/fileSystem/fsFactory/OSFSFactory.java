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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;

public class OSFSFactory implements FSFactory {
  private static final Logger logger = LoggerFactory.getLogger(OSFSFactory.class);

  @Override
  public File getFileWithParent(String pathname) {
    return null;
  }

  @Override
  public File getFile(String pathname) {
    return null;
  }

  @Override
  public File getFile(String parent, String child) {
    return null;
  }

  @Override
  public File getFile(File parent, String child) {
    return null;
  }

  @Override
  public File getFile(URI uri) {
    return null;
  }

  @Override
  public BufferedReader getBufferedReader(String filePath) {
    return null;
  }

  @Override
  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    return null;
  }

  @Override
  public BufferedInputStream getBufferedInputStream(String filePath) {
    return null;
  }

  @Override
  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    return null;
  }

  @Override
  public void moveFile(File srcFile, File destFile) {}

  @Override
  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    return new File[0];
  }

  @Override
  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    return new File[0];
  }

  @Override
  public boolean deleteIfExists(File file) throws IOException {
    return false;
  }

  @Override
  public void deleteDirectory(String dir) throws IOException {}
}
