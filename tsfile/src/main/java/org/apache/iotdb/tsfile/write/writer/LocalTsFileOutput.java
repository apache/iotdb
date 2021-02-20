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
package org.apache.iotdb.tsfile.write.writer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * a TsFileOutput implementation with FileOutputStream. If the file is not existed, it will be
 * created. Otherwise the file will be written from position 0.
 */
public class LocalTsFileOutput implements TsFileOutput {

  private FileOutputStream outputStream;
  private BufferedOutputStream bufferedStream;

  LocalTsFileOutput(File file, boolean append) throws FileNotFoundException {
    this.outputStream = new FileOutputStream(file, append);
    this.bufferedStream = new BufferedOutputStream(outputStream);
  }

  public LocalTsFileOutput(FileOutputStream outputStream) {
    this.outputStream = outputStream;
    this.bufferedStream = new BufferedOutputStream(outputStream);
  }

  @Override
  public void write(byte[] b) throws IOException {
    bufferedStream.write(b);
  }

  @Override
  public void write(byte b) throws IOException {
    bufferedStream.write(b);
  }

  @Override
  public void write(ByteBuffer b) throws IOException {
    bufferedStream.write(b.array());
  }

  @Override
  public long getPosition() throws IOException {
    bufferedStream.flush();
    return outputStream.getChannel().position();
  }

  @Override
  public void close() throws IOException {
    bufferedStream.close();
    outputStream.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return bufferedStream;
  }

  @Override
  public void flush() throws IOException {
    this.bufferedStream.flush();
  }

  @Override
  public void truncate(long size) throws IOException {
    outputStream.getChannel().truncate(size);
  }
}
