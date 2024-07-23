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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public enum WALFileVersion {
  V1("WAL"),
  V2("V2-WAL");

  private final String versionString;
  private byte[] versionBytes;

  public String getVersionString() {
    return versionString;
  }

  public byte[] getVersionBytes() {
    return versionBytes;
  }

  WALFileVersion(String versionString) {
    this.versionString = versionString;
    if (versionString != null) {
      this.versionBytes = versionString.getBytes(StandardCharsets.UTF_8);
    }
  }

  public static WALFileVersion getVersion(File file) throws IOException {
    try (FileChannel channel = FileChannel.open(file.toPath())) {
      return getVersion(channel);
    }
  }

  public static WALFileVersion getVersion(FileChannel channel) throws IOException {
    long originalPosition = channel.position();
    try {
      // head magic string starts to exist since V2
      WALFileVersion[] versions = {V2};
      for (WALFileVersion version : versions) {
        channel.position(0);
        if (channel.size() < version.versionBytes.length) {
          continue;
        }
        ByteBuffer buffer = ByteBuffer.allocate(version.versionBytes.length);
        channel.read(buffer);
        buffer.flip();
        String versionString = new String(buffer.array(), StandardCharsets.UTF_8);
        if (version.versionString.equals(versionString)) {
          return version;
        }
      }
      // v1 by default
      return V1;
    } finally {
      channel.position(originalPosition);
    }
  }
}
