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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DefaultTsFileInput implements TsFileInput {

  private static final Logger LOGGER = LoggerFactory
          .getLogger(DefaultTsFileInput.class);

  FileChannel channel;

  private Path path;

  public DefaultTsFileInput(Path file) throws IOException {
    channel = FileChannel.open(file, StandardOpenOption.READ);
    path = file;
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
//    if (!channel.isOpen()) {
//      LOGGER.error("File is closed while reading {}", path.toString());
//      channel = FileChannel.open(path, StandardOpenOption.READ);
//    }
    return channel.read(dst, position);
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
    LOGGER.error("{} FileChannel is closed", path);
//    Throwable ex = new Throwable();
//    StackTraceElement[] stackElements = ex.getStackTrace();
//    if (stackElements != null) {
//      for (StackTraceElement stackElement : stackElements) {
//        LOGGER.error("Class Name: {}, Function Name: {}, Line: {}", stackElement.getClassName(), stackElement.getMethodName(), stackElement.getLineNumber());
//      }
//    }
    channel.close();
  }

  @Override
  public int readInt() throws IOException {
    throw new UnsupportedOperationException();
  }
}
