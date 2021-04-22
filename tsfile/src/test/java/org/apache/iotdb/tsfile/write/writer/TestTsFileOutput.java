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

import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TestTsFileOutput implements TsFileOutput {

  PublicBAOS publicBAOS = new PublicBAOS();

  @Override
  public void write(byte[] b) throws IOException {
    publicBAOS.write(b);
  }

  @Override
  public void write(byte b) {
    publicBAOS.write(b);
  }

  @Override
  public void write(ByteBuffer b) {
    publicBAOS.write(b.array(), b.position(), b.limit());
  }

  @Override
  public long getPosition() {
    return publicBAOS.size();
  }

  @Override
  public void close() throws IOException {
    publicBAOS.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return publicBAOS;
  }

  @Override
  public void flush() throws IOException {
    publicBAOS.flush();
  }

  @Override
  public void truncate(long size) {
    publicBAOS.truncate((int) size);
  }
}
