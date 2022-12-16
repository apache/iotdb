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
package org.apache.iotdb.lsm.sstable.fileIO;

import org.apache.iotdb.lsm.sstable.bplustree.entry.IEntry;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileInput implements IFileInput {

  private String filePath;
  private FileInputStream fileInputStream;
  private FileChannel channel;
  private DataInputStream dataInputStream;

  public FileInput(File file) throws IOException {
    this.filePath = file.getAbsolutePath();
    fileInputStream = new FileInputStream(file);
    channel = fileInputStream.getChannel();
    dataInputStream = new DataInputStream(fileInputStream);
  }

  @Override
  public long size() throws IOException {
    return channel.size();
  }

  @Override
  public long position() throws IOException {
    return channel.position();
  }

  @Override
  public IFileInput position(long newPosition) throws IOException {
    channel.position(newPosition);
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return channel.read(dst);
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    return channel.read(dst, position);
  }

  @Override
  public int read() throws IOException {
    return dataInputStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return dataInputStream.read(b, off, len);
  }

  @Override
  public void read(IEntry entry, long offset) throws IOException {
    position(offset);
    entry.deserialize(dataInputStream);
  }

  @Override
  public void read(IEntry entry) throws IOException {
    entry.deserialize(dataInputStream);
  }

  @Override
  public FileChannel wrapAsFileChannel() throws IOException {
    return channel;
  }

  @Override
  public DataInputStream wrapAsInputStream() throws IOException {
    return dataInputStream;
  }

  @Override
  public void close() throws IOException {
    fileInputStream.close();
    channel.close();
    dataInputStream.close();
  }

  @Override
  public int readInt() throws IOException {
    return dataInputStream.readInt();
  }

  @Override
  public char readChar() throws IOException {
    return dataInputStream.readChar();
  }

  @Override
  public long readLong() throws IOException {
    return dataInputStream.readLong();
  }

  @Override
  public int skipBytes(int n) throws IOException {
    return dataInputStream.skipBytes(n);
  }

  @Override
  public String getFilePath() {
    return filePath;
  }
}
