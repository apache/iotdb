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
package org.apache.iotdb.tsfile.read.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DefaultTsFileInput implements TsFileInput {

  private static final Logger LOGGER = LoggerFactory
          .getLogger(DefaultTsFileInput.class);

  FileChannel channel;

  private String path;

  public DefaultTsFileInput(Path file) throws IOException {
    channel = FileChannel.open(file, StandardOpenOption.READ);
    path = file.toString();
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
    try {
      return channel.read(dst, position);
    } catch (ClosedChannelException e) {
      LOGGER.error(String.format("File is closed while reading %s", path));
      throw e;
    }
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileChannel wrapAsFileChannel() throws IOException {
    return channel;
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return Channels.newInputStream(channel);
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public int readInt() throws IOException {
    throw new UnsupportedOperationException();
  }
}
