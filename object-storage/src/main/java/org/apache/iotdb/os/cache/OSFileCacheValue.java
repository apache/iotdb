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

public class OSFileCacheValue {
  private String fileName;
  // 如果每个块用一个文件来存储，则该值一直为 0
  // 如果使用一个大文件存储所有块，则该值为大文件中的起点
  private long startPosition;
  // 如果每个块用一个文件来存储，则该值一直为该文件的大小
  // 如果使用一个大文件存储所有块，则该值为该块的实际长度
  private long length;

  public OSFileCacheValue(String fileName, long startPosition, long length) {
    this.fileName = fileName;
    this.startPosition = startPosition;
    this.length = length;
  }

  public String getFileName() {
    return fileName;
  }

  public long getStartPosition() {
    return startPosition;
  }

  public long getLength() {
    return length;
  }

  public long getOccupiedLength() {
    // 如果使用多个文件，则返回该文件的大小
    // 如果使用一个文件，则返回每个槽的大小
    return length;
  }
}
