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
package org.apache.iotdb.tsfile.io;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iotdb.tsfile.common.utils.ITsRandomAccessFileReader;

/**
 * This class is used to wrap the {@link}FSDataInputStream and implement the
 * interface {@link}TSRandomAccessFileReader.
 *
 * @author liukun
 */
public class HDFSInputStream implements ITsRandomAccessFileReader {

  private FSDataInputStream fsDataInputStream;
  private FileStatus fileStatus;

  public HDFSInputStream(String filePath) throws IOException {

    this(filePath, new Configuration());
  }

  public HDFSInputStream(String filePath, Configuration configuration) throws IOException {

    this(new Path(filePath), configuration);
  }

  public HDFSInputStream(Path path, Configuration configuration) throws IOException {

    FileSystem fs = FileSystem.get(configuration);
    fsDataInputStream = fs.open(path);
    fileStatus = fs.getFileStatus(path);
  }

  public void seek(long offset) throws IOException {

    fsDataInputStream.seek(offset);
  }

  public int read() throws IOException {

    return fsDataInputStream.read();
  }

  public long length() throws IOException {

    return fileStatus.getLen();
  }

  public int readInt() throws IOException {

    return fsDataInputStream.readInt();
  }

  public void close() throws IOException {

    fsDataInputStream.close();
  }

  public long getPos() throws IOException {

    return fsDataInputStream.getPos();
  }

  /**
   * Read the data into b, and the check the length
   *
   * @param b
   *            read the data into
   * @param off
   *            the begin offset to read into
   * @param len
   *            the length to read into
   */
  public int read(byte[] b, int off, int len) throws IOException {
    if (len < 0) {
      throw new IndexOutOfBoundsException();
    }
    int n = 0;
    while (n < len) {
      int count = fsDataInputStream.read(b, off + n, len - n);
      if (count < 0) {
        throw new IOException("The read length is out of the length of inputstream");
      }
      n += count;
    }
    return n;
  }
}