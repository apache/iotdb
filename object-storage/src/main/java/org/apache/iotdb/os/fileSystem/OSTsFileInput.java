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

import org.apache.iotdb.os.cache.OSFileCache;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class OSTsFileInput implements TsFileInput {
  private static final Logger logger = LoggerFactory.getLogger(OSTsFileInput.class);

  private String osFileName;
  private OSFileCache cache = OSFileCache.getInstance();

  @Override
  public long size() throws IOException {
    return 0;
  }

  @Override
  public long position() throws IOException {
    return 0;
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    return null;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {

    return 0;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    return 0;
  }

  @Override
  public int read() throws IOException {
    return 0;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return 0;
  }

  @Override
  public FileChannel wrapAsFileChannel() throws IOException {
    throw new UnsupportedEncodingException();
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public int readInt() throws IOException {
    return 0;
  }

  @Override
  public String readVarIntString(long offset) throws IOException {
    return null;
  }

  @Override
  public String getFilePath() {
    return null;
  }

  public InputStream getNextInputStream(long position) throws IOException {
    return cache.getAsInputSteam(osFileName, position);
  }
}
