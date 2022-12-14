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
package org.apache.iotdb.lsm.sstable.writer;

import org.apache.iotdb.lsm.sstable.bplustree.entry.IEntry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * a TsFileOutput implementation with FileOutputStream. If the file is not existed, it will be
 * created. Otherwise the file will be written from position 0.
 */
public class FileOutput extends OutputStream implements IFileOutput {

  private FileOutputStream outputStream;
  private ByteBuffer byteBuffer;
  private long position;

  public FileOutput(FileOutputStream outputStream, int bufferCapacity) {
    this.outputStream = outputStream;
    byteBuffer = ByteBuffer.allocate(bufferCapacity);
    position = 0;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    outputStream.write(b);
    position++;
  }

  @Override
  public synchronized void write(byte[] b) throws IOException {
    outputStream.write(b);
    position += b.length;
  }

  @Override
  public synchronized void write(byte b) throws IOException {
    outputStream.write(b);
    position++;
  }

  @Override
  public synchronized void write(byte[] buf, int start, int offset) throws IOException {
    outputStream.write(buf, start, offset);
    position += offset;
  }

  @Override
  public synchronized void write(ByteBuffer b) throws IOException {
    write(b.array(), 0, b.limit());
  }

  @Override
  public long write(IEntry entry) throws IOException {
    long startOffset = position;
    byteBuffer.clear();
    entry.serialize(byteBuffer);
    byteBuffer.flip();
    write(byteBuffer);
    return startOffset;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
    byteBuffer.clear();
    byteBuffer = null;
  }

  @Override
  public OutputStream wrapAsStream() {
    return this;
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void truncate(long size) throws IOException {
    outputStream.getChannel().truncate(size);
    position = outputStream.getChannel().position();
  }
}
