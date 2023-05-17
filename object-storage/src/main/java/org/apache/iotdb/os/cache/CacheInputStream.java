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
package org.apache.iotdb.os.cache;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class CacheInputStream extends InputStream {
  private final OSFileChannel channel;

  public CacheInputStream(OSFileChannel channel) {
    super();
    this.channel = channel;
  }

  @Override
  public int read() throws IOException {
    byte[] b1 = new byte[1];
    int n = read(b1);
    if (n == 1) return b1[0] & 0xff;
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    ByteBuffer buffer = ByteBuffer.wrap(b);
    buffer.position(off);
    buffer.limit(off + len);
    return channel.read(buffer);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    if (n > channel.size() - channel.position()) {
      n = channel.size() - channel.position();
    }
    channel.position(channel.position() + n);
    return n;
  }

  @Override
  public int available() throws IOException {
    return (int) (channel.size() - channel.position());
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported() {
    throw new UnsupportedOperationException();
  }
}
