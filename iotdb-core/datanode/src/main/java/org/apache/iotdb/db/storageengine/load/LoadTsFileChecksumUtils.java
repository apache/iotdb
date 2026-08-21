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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/** Deterministic content checksum helpers for consensus-backed LOAD pieces. */
public final class LoadTsFileChecksumUtils {

  private LoadTsFileChecksumUtils() {}

  /**
   * Computes a stable checksum for a list of {@link TsFileData}. The value only depends on the
   * serialized bytes, so any replica that applies the same consensus record observes the same
   * result.
   */
  public static long checksum(final List<TsFileData> dataList) {
    return checksum(0L, dataList);
  }

  /**
   * Computes the checksum of one consensus PIECE: the coordinator-assigned {@code pieceIndex} is
   * part of the digest, so two identical payloads at different indexes (or a reordered payload)
   * never produce the same checksum. Both the write node and every follower apply the same
   * function, keeping per-piece dedup, conflict detection and the PREPARE aggregate consistent.
   */
  public static long checksum(final long pieceIndex, final List<TsFileData> dataList) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final DataOutputStream stream =
          new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest));
      ReadWriteIOUtils.write(pieceIndex, stream);
      for (TsFileData data : dataList) {
        ReadWriteIOUtils.write(data.getType().ordinal(), stream);
        data.serialize(stream);
      }
      stream.flush();
      final byte[] bytes = digest.digest();
      long checksum = 0;
      for (int i = 0; i < Long.BYTES; i++) {
        checksum = (checksum << 8) | (bytes[i] & 0xFFL);
      }
      return checksum;
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Combines the next piece checksum into a running aggregate in a strictly order-sensitive way
   * (rotate-then-XOR). XOR alone is commutative, so it cannot detect pieces applied in a different
   * order; this rolling combination makes the PREPARE aggregate depend on the exact piece sequence,
   * which is the minimum requirement for a "fast" final consistency check.
   */
  public static long combine(final long aggregate, final long pieceChecksum) {
    return Long.rotateLeft(aggregate, 13) ^ pieceChecksum;
  }
}
