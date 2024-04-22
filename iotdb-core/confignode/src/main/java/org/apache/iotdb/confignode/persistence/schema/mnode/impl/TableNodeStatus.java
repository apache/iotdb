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

package org.apache.iotdb.confignode.persistence.schema.mnode.impl;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public enum TableNodeStatus {
  PRE_CREATE((byte) 0),

  USING((byte) 1),

  PRE_DELETE((byte) 2);

  private final byte status;

  private TableNodeStatus(byte status) {
    this.status = status;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(status, outputStream);
  }

  public static TableNodeStatus deserialize(InputStream inputStream) throws IOException {
    byte status = ReadWriteIOUtils.readByte(inputStream);
    switch (status) {
      case 0:
        return PRE_CREATE;
      case 1:
        return USING;
      case 2:
        return PRE_DELETE;
      default:
        throw new IllegalArgumentException();
    }
  }
}
