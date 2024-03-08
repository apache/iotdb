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

package org.apache.iotdb.db.storageengine.dataregion.compaction.io;

import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;

public class CompactionTsFileInput implements TsFileInput {
  private final TsFileInput tsFileInput;

  public CompactionTsFileInput(TsFileInput tsFileInput) {
    this.tsFileInput = tsFileInput;
  }

  @Override
  public long size() throws IOException {
    return tsFileInput.size();
  }

  @Override
  public long position() throws IOException {
    return tsFileInput.position();
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    return tsFileInput.position(newPosition);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int readSize = tsFileInput.read(dst);
    if (readSize == -1 && Thread.interrupted()) {
      throw new ClosedByInterruptException();
    }
    return readSize;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    int readSize = tsFileInput.read(dst, position);
    if (readSize == -1 && Thread.interrupted()) {
      throw new ClosedByInterruptException();
    }
    return readSize;
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return tsFileInput.wrapAsInputStream();
  }

  @Override
  public void close() throws IOException {
    tsFileInput.close();
  }

  @Override
  public String getFilePath() {
    return tsFileInput.getFilePath();
  }
}
