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

import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HybridFSFactory implements FSFactory {
  private static final Logger logger = LoggerFactory.getLogger(HybridFSFactory.class);
  private static final Map<FSType, FSFactory> fsFactories = new ConcurrentHashMap<>();

  static {
    fsFactories.put(FSType.LOCAL, new LocalFSFactory());
    fsFactories.put(FSType.HDFS, new HDFSFactory());
    fsFactories.put(FSType.OBJECT_STORAGE, new OSFSFactory());
  }

  @Override
  public File getFileWithParent(String pathname) {
    FSPath path = FSUtils.parse(pathname);
    return fsFactories.get(path.getFsType()).getFileWithParent(path.getPath());
  }

  @Override
  public File getFile(String pathname) {
    FSPath path = FSUtils.parse(pathname);
    return fsFactories.get(path.getFsType()).getFile(path.getPath());
  }

  @Override
  public File getFile(String parent, String child) {
    FSPath parentPath = FSUtils.parse(parent);
    return fsFactories.get(parentPath.getFsType()).getFile(parentPath.getPath(), child);
  }

  @Override
  public File getFile(File parent, String child) {
    FSType type = FSUtils.getFSType(parent);
    return fsFactories.get(type).getFile(parent, child);
  }

  @Override
  public File getFile(URI uri) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferedReader getBufferedReader(String filePath) {
    FSPath path = FSUtils.parse(filePath);
    return fsFactories.get(path.getFsType()).getBufferedReader(path.getPath());
  }

  @Override
  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    FSPath path = FSUtils.parse(filePath);
    return fsFactories.get(path.getFsType()).getBufferedWriter(path.getPath(), append);
  }

  @Override
  public BufferedInputStream getBufferedInputStream(String filePath) {
    FSPath path = FSUtils.parse(filePath);
    return fsFactories.get(path.getFsType()).getBufferedInputStream(path.getPath());
  }

  @Override
  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    FSPath path = FSUtils.parse(filePath);
    return fsFactories.get(path.getFsType()).getBufferedOutputStream(path.getPath());
  }

  @Override
  public void moveFile(File srcFile, File destFile) {
    // TODO
  }

  @Override
  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    FSPath folder = FSUtils.parse(fileFolder);
    return fsFactories.get(folder.getFsType()).listFilesBySuffix(folder.getPath(), suffix);
  }

  @Override
  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    FSPath folder = FSUtils.parse(fileFolder);
    return fsFactories.get(folder.getFsType()).listFilesByPrefix(folder.getPath(), prefix);
  }

  @Override
  public boolean deleteIfExists(File file) throws IOException {
    FSType type = FSUtils.getFSType(file);
    return fsFactories.get(type).deleteIfExists(file);
  }
}
