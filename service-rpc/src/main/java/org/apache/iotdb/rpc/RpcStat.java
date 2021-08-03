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

package org.apache.iotdb.rpc;

import java.util.concurrent.atomic.AtomicLong;

public class RpcStat {

  private RpcStat() {
    // pure static class
  }

  static final AtomicLong writeBytes = new AtomicLong();
  static final AtomicLong writeCompressedBytes = new AtomicLong();
  static final AtomicLong readBytes = new AtomicLong();
  static final AtomicLong readCompressedBytes = new AtomicLong();

  public static long getReadBytes() {
    return readBytes.get();
  }

  public static long getReadCompressedBytes() {
    return readCompressedBytes.get();
  }

  public static long getWriteBytes() {
    return writeBytes.get();
  }

  public static long getWriteCompressedBytes() {
    return writeCompressedBytes.get();
  }
}
