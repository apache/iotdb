/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.fileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

public class HdfsFile extends File {

  private Path hdfsPath;

  private static final Logger logger = LoggerFactory.getLogger(TsFileWriter.class);


  public HdfsFile(String pathname) {
    super(pathname);
    hdfsPath = new Path(pathname);
  }

  public HdfsFile(String parent, String child) {
    super(parent, child);
  }

  public HdfsFile(File parent, String child) {
    super(parent, child);
  }

  public HdfsFile(URI uri) {
    super(uri);
  }

  @Override
  public String getAbsolutePath() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public long length() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(new Configuration());
      return fs.getFileStatus(hdfsPath).getLen();
    } catch (IOException e) {
      logger.error("Fail to get length of the file. ", e);
      return 0;
    }
  }

  @Override
  public boolean exists() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(new Configuration());
      return fs.exists(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to check whether the file or directory exists. ", e);
      return false;
    }
  }

  @Override
  public File[] listFiles() {
    ArrayList<HdfsFile> files = new ArrayList<>();
    try {
      FileSystem fs = hdfsPath.getFileSystem(new Configuration());
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(hdfsPath, true);
      while (iterator.hasNext()) {
        LocatedFileStatus fileStatus = iterator.next();
        Path fullPath = fileStatus.getPath();
        files.add(new HdfsFile(fullPath.toUri()));
      }
      return files.toArray(new HdfsFile[files.size()]);
    } catch (IOException e) {
      logger.error("Fail to list files. ", e);
      return null;
    }
  }

  @Override
  public File[] listFiles(FileFilter filter) {
    ArrayList<HdfsFile> files = new ArrayList<>();
    try {
      PathFilter pathFilter = new GlobFilter(filter.toString()); // TODO
      FileSystem fs = hdfsPath.getFileSystem(new Configuration());
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(hdfsPath, true);
      while (iterator.hasNext()) {
        LocatedFileStatus fileStatus = iterator.next();
        Path fullPath = fileStatus.getPath();
        if (pathFilter.accept(fullPath)) {
          files.add(new HdfsFile(fullPath.toUri()));
        }
      }
      return files.toArray(new HdfsFile[files.size()]);
    } catch (IOException e) {
      logger.error("Fail to list files. ", e);
      return null;
    }
  }

  @Override
  public File getParentFile() {
    return new HdfsFile(hdfsPath.getParent().toUri());
  }

  @Override
  public boolean createNewFile() throws IOException {
    FileSystem fs = hdfsPath.getFileSystem(new Configuration());
    return fs.createNewFile(hdfsPath);
  }

  @Override
  public boolean delete() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(new Configuration());
      return fs.delete(hdfsPath, true);
    } catch (IOException e) {
      logger.error("Fail to delete file. ", e);
      return false;
    }
  }

  @Override
  public boolean mkdirs() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(new Configuration());
      return fs.mkdirs(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to create directory. ", e);
      return false;
    }
  }
}
