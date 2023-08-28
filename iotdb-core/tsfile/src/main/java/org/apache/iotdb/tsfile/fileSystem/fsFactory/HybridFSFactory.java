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

  private FSFactory getFSFactory(FSType fsType) {
    return fsFactories.compute(
        fsType,
        (k, v) -> {
          if (v != null) {
            return v;
          }
          switch (fsType) {
            case LOCAL:
              return new LocalFSFactory();
            case OBJECT_STORAGE:
              return new OSFSFactory();
            case HDFS:
              return new HDFSFactory();
            default:
              return null;
          }
        });
  }

  @Override
  public File getFileWithParent(String pathname) {
    FSPath path = FSUtils.parse(pathname);
    return getFSFactory(path.getFsType()).getFileWithParent(path.getPath());
  }

  @Override
  public File getFile(String pathname) {
    FSPath path = FSUtils.parse(pathname);
    return getFSFactory(path.getFsType()).getFile(path.getPath());
  }

  @Override
  public File getFile(String parent, String child) {
    FSPath parentPath = FSUtils.parse(parent);
    return getFSFactory(parentPath.getFsType()).getFile(parentPath.getPath(), child);
  }

  @Override
  public File getFile(File parent, String child) {
    FSType type = FSUtils.getFSType(parent);
    return getFSFactory(type).getFile(parent, child);
  }

  @Override
  public File getFile(URI uri) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferedReader getBufferedReader(String filePath) {
    FSPath path = FSUtils.parse(filePath);
    return getFSFactory(path.getFsType()).getBufferedReader(path.getPath());
  }

  @Override
  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    FSPath path = FSUtils.parse(filePath);
    return getFSFactory(path.getFsType()).getBufferedWriter(path.getPath(), append);
  }

  @Override
  public BufferedInputStream getBufferedInputStream(String filePath) {
    FSPath path = FSUtils.parse(filePath);
    return getFSFactory(path.getFsType()).getBufferedInputStream(path.getPath());
  }

  @Override
  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    FSPath path = FSUtils.parse(filePath);
    return getFSFactory(path.getFsType()).getBufferedOutputStream(path.getPath());
  }

  @Override
  public void moveFile(File srcFile, File destFile) throws IOException {
    FSType srcType = FSUtils.getFSType(srcFile);
    FSType destType = FSUtils.getFSType(destFile);
    if (srcType == destType) {
      getFSFactory(destType).moveFile(srcFile, destFile);
    } else {
      throw new IOException(
          String.format("Doesn't support move file from %s to %s.", srcType, destType));
    }
  }

  @Override
  public void copyFile(File srcFile, File destFile) throws IOException {
    FSType srcType = FSUtils.getFSType(srcFile);
    FSType destType = FSUtils.getFSType(destFile);
    if (srcType == destType || (srcType == FSType.LOCAL && destType == FSType.OBJECT_STORAGE)) {
      getFSFactory(destType).copyFile(srcFile, destFile);
    } else if ((srcType == FSType.LOCAL || srcType == FSType.HDFS)
        && (destType == FSType.LOCAL || destType == FSType.HDFS)) {
      getFSFactory(FSType.HDFS).copyFile(srcFile, destFile);
    } else {
      throw new IOException(
          String.format("Doesn't support move file from %s to %s.", srcType, destType));
    }
  }

  @Override
  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    FSPath folder = FSUtils.parse(fileFolder);
    return getFSFactory(folder.getFsType()).listFilesBySuffix(folder.getPath(), suffix);
  }

  @Override
  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    FSPath folder = FSUtils.parse(fileFolder);
    return getFSFactory(folder.getFsType()).listFilesByPrefix(folder.getPath(), prefix);
  }

  @Override
  public boolean deleteIfExists(File file) throws IOException {
    FSType type = FSUtils.getFSType(file);
    return getFSFactory(type).deleteIfExists(file);
  }

  @Override
  public void deleteDirectory(String dir) throws IOException {
    FSType type = FSUtils.getFSType(dir);
    getFSFactory(type).deleteDirectory(dir);
  }
}
