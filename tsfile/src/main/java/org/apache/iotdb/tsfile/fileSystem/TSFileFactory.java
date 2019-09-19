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

package org.apache.iotdb.tsfile.fileSystem;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

public enum TSFileFactory {

  INSTANCE;

  private static FSType fSType = TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs();
  private static final Logger logger = LoggerFactory.getLogger(TsFileWriter.class);
  private FileSystem fs;
  private Configuration conf = new Configuration();

  public File getFile(String pathname) {
    if (fSType.equals(fSType.HDFS)) {
      return new HDFSFile(pathname);
    } else {
      return new File(pathname);
    }
  }

  public File getFile(String parent, String child) {
    if (fSType.equals(fSType.HDFS)) {
      return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(File parent, String child) {
    if (fSType.equals(fSType.HDFS)) {
      return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(URI uri) {
    if (fSType.equals(fSType.HDFS)) {
      return new HDFSFile(uri);
    } else {
      return new File(uri);
    }
  }

  public BufferedReader getBufferedReader(String filePath) {
    try {
      if (fSType.equals(fSType.HDFS)) {
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
      if (fSType.equals(fSType.HDFS)) {
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
      if (fSType.equals(fSType.HDFS)) {
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
      if (fSType.equals(fSType.HDFS)) {
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
      if (fSType.equals(fSType.HDFS)) {
        boolean rename = srcFile.renameTo(destFile);
        if (!rename) {
          logger.error("Failed to rename file from {} to {}. ", srcFile.getName(), destFile.getName());
        }
      } else {
        FileUtils.moveFile(srcFile, destFile);
      }
    } catch (IOException e) {
      logger.error("Failed to move file from {} to {}. ", srcFile.getAbsolutePath(), destFile.getAbsolutePath(), e);
    }
  }
}