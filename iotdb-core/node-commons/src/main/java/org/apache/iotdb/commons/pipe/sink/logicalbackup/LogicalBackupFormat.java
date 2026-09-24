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

package org.apache.iotdb.commons.pipe.sink.logicalbackup;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.zip.CRC32C;

public final class LogicalBackupFormat {

  public static final String FORMAT_NAME = "iotdb-pipe-logical-backup";
  public static final String FORMAT_VERSION = "1.0";
  public static final short MAJOR_VERSION = 1;
  public static final short MINOR_VERSION = 0;
  public static final long SEGMENT_MAGIC = 0x494F545057414C31L;
  public static final int RECORD_MAGIC = 0x50575231;
  public static final int FOOTER_MAGIC = 0x50575346;
  public static final int SEGMENT_HEADER_SIZE = 32;
  public static final int RECORD_HEADER_SIZE = 56;
  public static final int SEGMENT_FOOTER_SIZE = 96;
  public static final int SHA256_SIZE = 32;
  public static final int MAX_RECORD_BYTES = 256 * 1024 * 1024;
  public static final String MANIFEST_FILE_NAME = "manifest.json";

  private LogicalBackupFormat() {}

  public static int crc32c(final byte[] bytes, final int offset, final int length) {
    final CRC32C crc32c = new CRC32C();
    crc32c.update(bytes, offset, length);
    return (int) crc32c.getValue();
  }

  public static MessageDigest newSha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String toHex(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }
}
