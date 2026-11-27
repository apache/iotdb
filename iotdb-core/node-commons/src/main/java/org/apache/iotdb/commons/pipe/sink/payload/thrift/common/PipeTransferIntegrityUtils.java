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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.commons.pipe.sink.payload.thrift.common;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.zip.CRC32;

public final class PipeTransferIntegrityUtils {

  public static final String STRUCTURE_CORRUPTION_MARKER = "[TSFILE_STRUCTURE_CORRUPTION]";
  public static final String CRC_MISMATCH_MARKER = "[PIPE_TRANSFER_CRC_MISMATCH]";

  private PipeTransferIntegrityUtils() {}

  public static void setCrc32(final TPipeTransferReq req) {
    req.setCrc32(calculateCrc32(req));
  }

  public static long calculateCrc32(final TPipeTransferReq req) {
    final CRC32 crc32 = new CRC32();
    crc32.update(req.getVersion());
    crc32.update((req.getType() >>> 8) & 0xFF);
    crc32.update(req.getType() & 0xFF);
    final byte[] body = req.getBody();
    crc32.update(body, 0, body.length);
    return crc32.getValue();
  }

  public static String calculateSha256(final File file) throws IOException {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IOException(e);
    }

    final byte[] buffer = new byte[8192];
    try (final InputStream inputStream = Files.newInputStream(file.toPath())) {
      int length;
      while ((length = inputStream.read(buffer)) != -1) {
        digest.update(buffer, 0, length);
      }
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  public static String calculateSha256OrUnavailable(final File file, final String unavailableText) {
    try {
      return calculateSha256(file);
    } catch (final IOException e) {
      return unavailableText;
    }
  }

  public static boolean isStructureCorruption(final String message) {
    return message != null && message.contains(STRUCTURE_CORRUPTION_MARKER);
  }

  public static boolean isCrcMismatch(final String message) {
    return message != null && message.contains(CRC_MISMATCH_MARKER);
  }
}
