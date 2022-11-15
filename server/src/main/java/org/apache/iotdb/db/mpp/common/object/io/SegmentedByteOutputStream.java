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

package org.apache.iotdb.db.mpp.common.object.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SegmentedByteOutputStream extends OutputStream {

  private final List<ByteBuffer> bufferList = new ArrayList<>();

  private final int bufferSize;

  private ByteBuffer workingBuffer;

  public SegmentedByteOutputStream(int bufferSize) {
    this.bufferSize = bufferSize;
    workingBuffer = ByteBuffer.allocate(bufferSize);
  }

  @Override
  public void write(int b) throws IOException {
    if (!workingBuffer.hasRemaining()) {
      getNewBuffer();
    }
    workingBuffer.put((byte) b);
  }

  public List<ByteBuffer> getBufferList() {
    getNewBuffer();
    return bufferList;
  }

  private void getNewBuffer() {
    workingBuffer.flip();
    bufferList.add(workingBuffer);
    workingBuffer = ByteBuffer.allocate(bufferSize);
  }
}
