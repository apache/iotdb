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
package org.apache.iotdb.commons.path;

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public enum PathType {
  Measurement((byte) 0),
  Aligned((byte) 1),
  Partial((byte) 2),
  Path((byte) 3);

  private final byte pathType;

  PathType(byte pathType) {
    this.pathType = pathType;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(pathType, buffer);
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(pathType, stream);
  }

  public void serialize(PublicBAOS stream) throws IOException {
    ReadWriteIOUtils.write(pathType, stream);
  }
}
