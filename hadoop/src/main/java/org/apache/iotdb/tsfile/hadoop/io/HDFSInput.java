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
package org.apache.iotdb.tsfile.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class is used to wrap the {@link}FSDataInputStream and implement the
 * interface {@link}TsFileInput.
 *
 * @author Yuan Tian
 */
public class HDFSInput implements TsFileInput {

  private FSDataInputStream fsDataInputStream;
  private FileStatus fileStatus;

  public HDFSInput(String filePath) throws IOException {
    this(filePath, new Configuration());
  }

  public HDFSInput(String filePath, Configuration configuration) throws IOException {
    this(new Path(filePath), configuration);
  }

  public HDFSInput(Path path, Configuration configuration) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);
    fsDataInputStream = fs.open(path);
    fileStatus = fs.getFileStatus(path);
  }

  @Override
  public long size() {
    return fileStatus.getLen();
  }

  @Override
  public long position() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    fsDataInputStream.seek(newPosition);
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int res;
    try {
      res = fsDataInputStream.read(dst);
    } catch (UnsupportedOperationException e) {
      // In case of Byte-buffer read unsupported by input stream
      byte[] bytes = new byte[dst.remaining()];
      res = fsDataInputStream.read(bytes);
      dst.put(bytes);
    }
    return res;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("position must be non-negative");
    }
    if (position >= this.size()) {
      return -1;
    }

    // record the before position
    long srcPosition = fsDataInputStream.getPos();

    fsDataInputStream.seek(position);
    // read data from inputStream to dst
    int res = read(dst);
    // recover to the before position
    fsDataInputStream.seek(srcPosition);
    return res;
  }

  @Override
  public int read() throws IOException {
    return fsDataInputStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return fsDataInputStream.read(off, b, 0, len);
  }

  @Override
  public FileChannel wrapAsFileChannel() throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return fsDataInputStream;
  }

  @Override
  public void close() throws IOException {
    fsDataInputStream.close();
  }

  @Override
  public int readInt() throws IOException {
    throw new IOException("Not support");
  }
}