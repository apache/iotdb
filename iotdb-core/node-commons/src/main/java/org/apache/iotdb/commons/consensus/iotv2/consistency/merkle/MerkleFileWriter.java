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

package org.apache.iotdb.commons.consensus.iotv2.consistency.merkle;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Writes a .merkle sidecar file alongside a TsFile. Format version 2 supports dual-digest
 * (fileXorHash + fileAddHash) in the header.
 */
public class MerkleFileWriter {

  private static final byte[] MAGIC = {'M', 'R', 'K', 'L'};
  private static final short VERSION = 2;
  private static final short FLAGS = 0;

  private MerkleFileWriter() {}

  /**
   * Write a .merkle file at the specified path.
   *
   * @param merklePath full path for the .merkle file (typically tsFilePath + ".merkle")
   * @param entries all (device, measurement, timeBucket) hash entries sorted by device -> meas ->
   *     timeBucketStart
   * @param fileXorHash XOR of all entry hashes
   * @param fileAddHash SUM of all entry hashes (mod 2^64)
   */
  public static void write(
      String merklePath, List<MerkleEntry> entries, long fileXorHash, long fileAddHash)
      throws IOException {
    CRC32 crc32 = new CRC32();
    try (OutputStream fos = new FileOutputStream(merklePath);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        CrcDataOutputStream out = new CrcDataOutputStream(bos, crc32)) {

      // Header (28 bytes)
      out.write(MAGIC);
      out.writeShort(VERSION);
      out.writeShort(FLAGS);
      out.writeLong(fileXorHash);
      out.writeLong(fileAddHash);
      out.writeInt(entries.size());

      // Entries
      for (MerkleEntry entry : entries) {
        byte[] deviceBytes = entry.getDeviceId().getBytes(StandardCharsets.UTF_8);
        out.writeShort(deviceBytes.length);
        out.write(deviceBytes);

        byte[] measurementBytes = entry.getMeasurement().getBytes(StandardCharsets.UTF_8);
        out.writeShort(measurementBytes.length);
        out.write(measurementBytes);

        out.writeLong(entry.getTimeBucketStart());
        out.writeLong(entry.getTimeBucketEnd());
        out.writeInt(entry.getPointCount());
        out.writeLong(entry.getEntryHash());
      }

      // Footer: CRC32 checksum
      out.flush();
      int crc = (int) crc32.getValue();
      // Write CRC without updating the CRC itself
      bos.write((crc >>> 24) & 0xFF);
      bos.write((crc >>> 16) & 0xFF);
      bos.write((crc >>> 8) & 0xFF);
      bos.write(crc & 0xFF);
    }
  }

  /** Computes the file-level dual-digest from a list of entries. */
  public static long computeFileXorHash(List<MerkleEntry> entries) {
    long xor = 0;
    for (MerkleEntry entry : entries) {
      xor ^= entry.getEntryHash();
    }
    return xor;
  }

  public static long computeFileAddHash(List<MerkleEntry> entries) {
    long add = 0;
    for (MerkleEntry entry : entries) {
      add += entry.getEntryHash();
    }
    return add;
  }

  /**
   * A DataOutputStream wrapper that updates a CRC32 on every byte written, so we can compute the
   * checksum in a single pass.
   */
  private static class CrcDataOutputStream extends DataOutputStream {
    private final CRC32 crc;

    CrcDataOutputStream(OutputStream out, CRC32 crc) {
      super(out);
      this.crc = crc;
    }

    @Override
    public synchronized void write(int b) throws IOException {
      super.write(b);
      crc.update(b);
    }

    @Override
    public synchronized void write(byte[] buf, int off, int len) throws IOException {
      super.write(buf, off, len);
      crc.update(buf, off, len);
    }
  }
}
