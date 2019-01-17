/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * A subclass extending <code>ByteArrayOutputStream</code>. It's used to return the byte array directly. Note that the
 * size of byte array is large than actual size of valid contents, thus it's used cooperating with <code>size()</code>
 * or <code>capacity = size</code>
 */
public class PublicBAOS extends ByteArrayOutputStream {

  public PublicBAOS(int size) {
    super(size);
  }

  public PublicBAOS() {
    super();
  }

  /**
   * get current all bytes data
   *
   * @return all bytes data
   */
  public byte[] getBuf() {

    return this.buf;
  }

  /**
   * Construct one {@link ByteArrayInputStream} from the buff data
   *
   * @return one {@link ByteArrayInputStream} have all buff data
   */
  public ByteArrayInputStream transformToInputStream() {
    return new ByteArrayInputStream(this.buf, 0, size());
  }

}
