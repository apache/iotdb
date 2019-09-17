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
      logger.error("Fail to get buffered reader. ", e);
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
      logger.error("Fail to get buffered writer. ", e);
      return null;
    }
  }

}