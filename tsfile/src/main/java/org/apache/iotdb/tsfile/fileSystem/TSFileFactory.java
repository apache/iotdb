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

package org.apache.iotdb.tsfile.fileSystem;

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
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum TSFileFactory {

  INSTANCE;

  private static FSType fSType = TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs();
  private static final Logger logger = LoggerFactory.getLogger(TSFileFactory.class);
  private FileSystem fs;
  private Configuration conf = new Configuration();

  public File getFile(String pathname) {
    if (fSType.equals(FSType.HDFS)) {
      return new HDFSFile(pathname);
    } else {
      return new File(pathname);
    }
  }

  public File getFile(String parent, String child) {
    if (fSType.equals(FSType.HDFS)) {
      return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(File parent, String child) {
    if (fSType.equals(FSType.HDFS)) {
      return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(URI uri) {
    if (fSType.equals(FSType.HDFS)) {
      return new HDFSFile(uri);
    } else {
      return new File(uri);
    }
  }

  public BufferedReader getBufferedReader(String filePath) {
    try {
      if (fSType.equals(FSType.HDFS)) {
        Path path = new Path(filePath);
        fs = path.getFileSystem(conf);
        return new BufferedReader(new InputStreamReader(fs.open(path)));
      } else {
        return new BufferedReader(new FileReader(filePath));
      }
    } catch (IOException e) {
      logger.error("Failed to get buffered reader for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    try {
      if (fSType.equals(FSType.HDFS)) {
        Path path = new Path(filePath);
        fs = path.getFileSystem(conf);
        return new BufferedWriter(new OutputStreamWriter(fs.create(path)));
      } else {
        return new BufferedWriter(new FileWriter(filePath, append));
      }
    } catch (IOException e) {
      logger.error("Failed to get buffered writer for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedInputStream getBufferedInputStream(String filePath) {
    try {
      if (fSType.equals(FSType.HDFS)) {
        Path path = new Path(filePath);
        fs = path.getFileSystem(conf);
        return new BufferedInputStream(fs.open(path));
      } else {
        return new BufferedInputStream(new FileInputStream(filePath));
      }
    } catch (IOException e) {
      logger.error("Failed to get buffered input stream for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    try {
      if (fSType.equals(FSType.HDFS)) {
        Path path = new Path(filePath);
        fs = path.getFileSystem(conf);
        return new BufferedOutputStream(fs.create(path));
      } else {
        return new BufferedOutputStream(new FileOutputStream(filePath));
      }
    } catch (IOException e) {
      logger.error("Failed to get buffered output stream for {}. ", filePath, e);
      return null;
    }
  }

  public void moveFile(File srcFile, File destFile) {
    try {
      if (fSType.equals(FSType.HDFS)) {
        boolean rename = srcFile.renameTo(destFile);
        if (!rename) {
          logger.error("Failed to rename file from {} to {}. ", srcFile.getName(),
              destFile.getName());
        }
      } else {
        FileUtils.moveFile(srcFile, destFile);
      }
    } catch (IOException e) {
      logger.error("Failed to move file from {} to {}. ", srcFile.getAbsolutePath(),
          destFile.getAbsolutePath(), e);
    }
  }

  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    if (fSType.equals(FSType.HDFS)) {
      PathFilter pathFilter = path -> path.toUri().toString().endsWith(suffix);
      List<HDFSFile> files = listFiles(fileFolder, pathFilter);
      return files.toArray(new HDFSFile[files.size()]);
    } else {
      return new File(fileFolder).listFiles(file -> file.getName().endsWith(suffix));
    }
  }

  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    if (fSType.equals(FSType.HDFS)) {
      PathFilter pathFilter = path -> path.toUri().toString().startsWith(prefix);
      List<HDFSFile> files = listFiles(fileFolder, pathFilter);
      return files.toArray(new HDFSFile[files.size()]);
    } else {
      return new File(fileFolder).listFiles(file -> file.getName().startsWith(prefix));
    }
  }

  private List<HDFSFile> listFiles(String fileFolder, PathFilter pathFilter) {
    List<HDFSFile> files = new ArrayList<>();
    try {
      Path path = new Path(fileFolder);
      fs = path.getFileSystem(conf);
      for (FileStatus fileStatus: fs.listStatus(path)) {
        Path filePath = fileStatus.getPath();
        if (pathFilter.accept(filePath)) {
          files.add(new HDFSFile(filePath.toUri().toString()));
        }
      }
    } catch (IOException e) {
      logger.error("Failed to list files in {}. ", fileFolder);
    }
    return files;
  }
}