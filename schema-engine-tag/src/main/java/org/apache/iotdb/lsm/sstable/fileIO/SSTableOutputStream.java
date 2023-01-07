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

import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * a FileOutput implementation with FileOutputStream. If the file is not existed, it will be
 * created. Otherwise the file will be written from position 0.
 */
public class SSTableOutputStream extends OutputStream implements ISSTableOutputStream {

  private final DataOutputStream dataOutputStream;
  private long position;

  public SSTableOutputStream(File file) throws FileNotFoundException {
    this(new FileOutputStream(file, true));
  }

  public SSTableOutputStream(FileOutputStream outputStream) {
    dataOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));
    position = 0;
  }

  @Override
  public void writeInt(int v) throws IOException {
    dataOutputStream.writeInt(v);
    position += 4;
  }

  @Override
  public void write(int b) throws IOException {
    dataOutputStream.write(b);
    position += 1;
  }

  @Override
  public void write(byte[] b) throws IOException {
    dataOutputStream.write(b);
    position += b.length;
  }

  @Override
  public void write(byte[] buf, int start, int offset) throws IOException {
    dataOutputStream.write(buf, start, offset);
    position += offset;
  }

  @Override
  public long write(IDiskEntry entry) throws IOException {
    long startOffset = position;
    int len = entry.serialize(dataOutputStream);
    position += len;
    return startOffset;
  }

  @Override
  public int writeAndGetSize(IDiskEntry entry) throws IOException {
    int len = entry.serialize(dataOutputStream);
    position += len;
    return len;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public void close() throws IOException {
    try {
      flush();
    } finally {
      dataOutputStream.close();
    }
  }

  @Override
  public void flush() throws IOException {
    if (dataOutputStream != null) {
      dataOutputStream.flush();
    }
  }
}
