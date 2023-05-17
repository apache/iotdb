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
package org.apache.iotdb.os.fileSystem;

import org.apache.iotdb.os.cache.OSFileChannel;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class OSTsFileInput implements TsFileInput {
  private OSFile file;
  private OSFileChannel channel;

  public OSTsFileInput(OSFile file) throws IOException {
    this.file = file;
    this.channel = new OSFileChannel(file);
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
  public TsFileInput position(long newPosition) throws IOException {
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
  public InputStream wrapAsInputStream() throws IOException {
    return OSFileChannel.newInputStream(channel);
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public String getFilePath() {
    return file.getPath();
  }
}
