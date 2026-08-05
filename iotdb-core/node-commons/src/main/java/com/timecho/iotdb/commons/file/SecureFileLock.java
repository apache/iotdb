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

package com.timecho.iotdb.commons.file;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

final class SecureFileLock extends FileLock {

  private final FileLock delegate;

  SecureFileLock(FileChannel owner, FileLock delegate) {
    super(owner, delegate.position(), delegate.size(), delegate.isShared());
    this.delegate = delegate;
  }

  SecureFileLock(AsynchronousFileChannel owner, FileLock delegate) {
    super(owner, delegate.position(), delegate.size(), delegate.isShared());
    this.delegate = delegate;
  }

  @Override
  public boolean isValid() {
    return delegate.isValid();
  }

  @Override
  public void release() throws IOException {
    delegate.release();
  }
}
