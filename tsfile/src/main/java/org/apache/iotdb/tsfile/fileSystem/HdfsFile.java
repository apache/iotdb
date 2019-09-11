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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;

public class HdfsFile extends File {

  private Path hdfsPath;
  private Configuration conf = new Configuration();
  private static final Logger logger = LoggerFactory.getLogger(TsFileWriter.class);


  public HdfsFile(String pathname) {
    super(pathname);
    hdfsPath = new Path(pathname);
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  }

  public HdfsFile(String parent, String child) {
    super(parent, child);
    hdfsPath = new Path(parent + child);
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  }

  public HdfsFile(File parent, String child) {
    super(parent, child);
    hdfsPath = new Path(parent.getAbsolutePath() + child);
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  }

  public HdfsFile(URI uri) {
    super(uri);
    hdfsPath = new Path(uri);
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  }

  @Override
  public String getAbsolutePath() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public String getPath() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public long length() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(conf);
      return fs.getFileStatus(hdfsPath).getLen();
    } catch (IOException e) {
      logger.error("Fail to get length of the file. ", e);
      return 0;
    }
  }

  @Override
  public boolean exists() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(conf);
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
      FileSystem fs = hdfsPath.getFileSystem(conf);
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
      FileSystem fs = hdfsPath.getFileSystem(conf);
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
    FileSystem fs = hdfsPath.getFileSystem(conf);
    return fs.createNewFile(hdfsPath);
  }

  @Override
  public boolean delete() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(conf);
      return fs.delete(hdfsPath, true);
    } catch (IOException e) {
      logger.error("Fail to delete file. ", e);
      return false;
    }
  }

  @Override
  public boolean mkdirs() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(conf);
      return fs.mkdirs(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to create directory. ", e);
      return false;
    }
  }

  @Override
  public boolean isDirectory() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(conf);
      return fs.getFileStatus(hdfsPath).isDirectory();
    } catch (IOException e) {
      logger.error("Fail to judge whether it is a directory. ", e);
      return false;
    }
  }

  @Override
  public long getFreeSpace() {
    try {
      FileSystem fs = hdfsPath.getFileSystem(conf);
      return fs.getStatus().getRemaining();
    } catch (IOException e) {
      logger.error("Fail to get free space. ", e);
      return 0L;
    }
  }

  @Override
  public String getName() {
    return hdfsPath.getName();
  }

  @Override
  public String toString() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public int hashCode() {
    return hdfsPath.hashCode();
  }

  @Override
  public String getParent() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean isAbsolute() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public File getAbsoluteFile() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public String getCanonicalPath() throws IOException {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public File getCanonicalFile() throws IOException {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public URL toURL() throws MalformedURLException {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public URI toURI() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean canRead() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean canWrite() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean isFile() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean isHidden() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public long lastModified() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public void deleteOnExit() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public String[] list() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public String[] list(FilenameFilter filter) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public File[] listFiles(FilenameFilter filter) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean mkdir() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean renameTo(File dest) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setLastModified(long time) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setReadOnly() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setWritable(boolean writable, boolean ownerOnly) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setWritable(boolean writable) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setReadable(boolean readable, boolean ownerOnly) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setReadable(boolean readable) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setExecutable(boolean executable, boolean ownerOnly) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean setExecutable(boolean executable) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean canExecute() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public long getTotalSpace() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public long getUsableSpace() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public int compareTo(File pathname) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public boolean equals(Object obj) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public java.nio.file.Path toPath() {
    throw new UnsupportedOperationException("Unsupported operation.");
  }
}
