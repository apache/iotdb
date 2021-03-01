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
package org.apache.iotdb.hadoop.fileSystem;

import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This class is used to wrap the {@link}FSDataOutputStream and implement the interface
 * {@link}TsFileOutput
 */
public class HDFSOutput implements TsFileOutput {

  private FSDataOutputStream fsDataOutputStream;
  private FileSystem fs;
  private Path path;

  public HDFSOutput(String filePath, boolean overwrite) throws IOException {
    this(filePath, new Configuration(), overwrite);
    path = new Path(filePath);
  }

  public HDFSOutput(String filePath, Configuration configuration, boolean overwrite)
      throws IOException {
    this(new Path(filePath), configuration, overwrite);
    path = new Path(filePath);
  }

  public HDFSOutput(Path path, Configuration configuration, boolean overwrite) throws IOException {
    fs = path.getFileSystem(HDFSConfUtil.setConf(configuration));
    fsDataOutputStream = fs.exists(path) ? fs.append(path) : fs.create(path, overwrite);
    this.path = path;
  }

  @Override
  public void write(byte[] b) throws IOException {
    fsDataOutputStream.write(b);
  }

  @Override
  public void write(byte b) throws IOException {
    fsDataOutputStream.write(b);
  }

  @Override
  public void write(ByteBuffer b) {
    throw new UnsupportedOperationException("Unsupported operation.");
  }

  @Override
  public long getPosition() throws IOException {
    return fsDataOutputStream.getPos();
  }

  @Override
  public void close() throws IOException {
    fsDataOutputStream.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return fsDataOutputStream;
  }

  @Override
  public void flush() throws IOException {
    this.fsDataOutputStream.hflush();
  }

  @Override
  public void truncate(long size) throws IOException {
    if (fs.exists(path)) {
      fsDataOutputStream.close();
    }
    fs.truncate(path, size);
    if (fs.exists(path)) {
      fsDataOutputStream = fs.append(path);
    }
  }
}
