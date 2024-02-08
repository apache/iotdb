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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * a TsFileOutput implementation with FileOutputStream. If the file is not existed, it will be
 * created. Otherwise the file will be written from position 0.
 */
public class LocalTsFileOutput extends OutputStream implements TsFileOutput {

  private FileOutputStream outputStream;
  private BufferedOutputStream bufferedStream;
  private long position;

  public LocalTsFileOutput(FileOutputStream outputStream) {
    this.outputStream = outputStream;
    this.bufferedStream = new BufferedOutputStream(outputStream);
    position = 0;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    bufferedStream.write(b);
    position++;
  }

  @Override
  public synchronized void write(byte[] b) throws IOException {
    bufferedStream.write(b);
    position += b.length;
  }

  @Override
  public synchronized void write(byte b) throws IOException {
    bufferedStream.write(b);
    position++;
  }

  @Override
  public synchronized void write(byte[] buf, int start, int offset) throws IOException {
    bufferedStream.write(buf, start, offset);
    position += offset;
  }

  @Override
  public synchronized void write(ByteBuffer b) throws IOException {
    bufferedStream.write(b.array());
    position += b.array().length;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public void close() throws IOException {
    bufferedStream.close();
    outputStream.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return this;
  }

  @Override
  public void flush() throws IOException {
    this.bufferedStream.flush();
  }

  @Override
  public void truncate(long size) throws IOException {
    bufferedStream.flush();
    outputStream.getChannel().truncate(size);
    position = outputStream.getChannel().position();
  }
}
