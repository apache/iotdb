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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;


/**
 * This class is used to wrap the {@link}FSDataOutputStream and implement the interface
 * {@link}TsFileOutput
 */
public class HDFSOutput implements TsFileOutput {

  private FSDataOutputStream fsDataOutputStream;

  public HDFSOutput(String filePath, boolean overwriter) throws IOException {

    this(filePath, new Configuration(), overwriter);
  }


  public HDFSOutput(String filePath, Configuration configuration, boolean overwriter)
      throws IOException {

    this(new Path(filePath), configuration, overwriter);
  }

  public HDFSOutput(Path path, Configuration configuration, boolean overwriter)
      throws IOException {
    FileSystem fs = path.getFileSystem(configuration);
    fsDataOutputStream = fs.create(path, overwriter);
  }

  @Override
  public void write(byte[] b) throws IOException {

    fsDataOutputStream.write(b);
  }

  public void write(ByteBuffer b) throws IOException {
    throw new IOException("Not support");
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
  public OutputStream wrapAsStream() throws IOException {
    return fsDataOutputStream;
  }

  @Override
  public void flush() throws IOException {
    this.fsDataOutputStream.flush();
  }

  @Override
  public void truncate(long position) throws IOException {
    throw new IOException("Not support");
  }
}
