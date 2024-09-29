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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.tsfile.utils.BytesUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RewritableByteArrayOutputStream extends ByteArrayOutputStream {

  private static final byte[] intPlaceHolder = new byte[4];
  private final int limit;

  public RewritableByteArrayOutputStream(final int limit) {
    this.limit = limit;
  }

  public synchronized boolean tryWrite(final byte[] content) throws IOException {
    if (content.length + count <= limit) {
      write(content);
      return true;
    }
    return false;
  }

  public synchronized int skipInt() throws IOException {
    final int result = count;
    write(intPlaceHolder);
    return result;
  }

  public synchronized void rewrite(final int n, final int off) {
    byte[] bytes = BytesUtils.intToBytes(n);
    if ((off < 0) || (off > buf.length - bytes.length)) {
      throw new IndexOutOfBoundsException();
    }
    System.arraycopy(bytes, 0, buf, off, bytes.length);
  }

  public int getCount() {
    return count;
  }
}
