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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/** Reads and validates a .merkle sidecar file. Supports version 2 format with dual-digest. */
public class MerkleFileReader {

  private static final byte[] MAGIC = {'M', 'R', 'K', 'L'};

  private MerkleFileReader() {}

  /**
   * Read a .merkle file and return its in-memory content.
   *
   * @param merklePath full path to the .merkle file
   * @param sourceTsFilePath the associated TsFile path (for attribution)
   * @return parsed content, or throws on corruption
   */
  public static MerkleFileContent read(String merklePath, String sourceTsFilePath)
      throws IOException {
    CRC32 crc32 = new CRC32();
    try (InputStream fis = new FileInputStream(merklePath);
        BufferedInputStream bis = new BufferedInputStream(fis);
        CrcInputStream crcStream = new CrcInputStream(bis, crc32);
        DataInputStream in = new DataInputStream(crcStream)) {

      // Header: magic (4), version (2), flags (2), fileXorHash (8), fileAddHash (8), entryCount
      // (4)
      byte[] magic = new byte[4];
      in.readFully(magic);
      if (magic[0] != MAGIC[0]
          || magic[1] != MAGIC[1]
          || magic[2] != MAGIC[2]
          || magic[3] != MAGIC[3]) {
        throw new IOException("Invalid .merkle file magic: " + merklePath);
      }

      short version = in.readShort();
      if (version < 2) {
        throw new IOException("Unsupported .merkle version: " + version + " at " + merklePath);
      }
      in.readShort(); // flags, reserved

      long fileXorHash = in.readLong();
      long fileAddHash = in.readLong();
      int entryCount = in.readInt();

      List<MerkleEntry> entries = new ArrayList<>(entryCount);
      for (int i = 0; i < entryCount; i++) {
        short deviceIdLen = in.readShort();
        byte[] deviceIdBytes = new byte[deviceIdLen];
        in.readFully(deviceIdBytes);
        String deviceId = new String(deviceIdBytes, StandardCharsets.UTF_8);

        short measurementLen = in.readShort();
        byte[] measurementBytes = new byte[measurementLen];
        in.readFully(measurementBytes);
        String measurement = new String(measurementBytes, StandardCharsets.UTF_8);

        long timeBucketStart = in.readLong();
        long timeBucketEnd = in.readLong();
        int pointCount = in.readInt();
        long entryHash = in.readLong();

        entries.add(
            new MerkleEntry(
                deviceId, measurement, timeBucketStart, timeBucketEnd, pointCount, entryHash));
      }

      // Footer: CRC32 validation -- capture CRC before reading the footer
      int computedCrc = (int) crc32.getValue();
      // Read the stored CRC via DataInputStream (this will update crc32, but we already saved it)
      int storedCrc = in.readInt();
      if (computedCrc != storedCrc) {
        throw new IOException(
            String.format(
                "CRC mismatch in .merkle file %s: expected 0x%08X, got 0x%08X",
                merklePath, storedCrc, computedCrc));
      }

      return new MerkleFileContent(fileXorHash, fileAddHash, entries, sourceTsFilePath);
    }
  }

  /**
   * An InputStream wrapper that updates a CRC32 on every byte read. DataInputStream is then
   * constructed on top of this, ensuring all reads flow through the CRC update.
   */
  private static class CrcInputStream extends InputStream {
    private final InputStream delegate;
    private final CRC32 crc;

    CrcInputStream(InputStream delegate, CRC32 crc) {
      this.delegate = delegate;
      this.crc = crc;
    }

    @Override
    public int read() throws IOException {
      int b = delegate.read();
      if (b >= 0) {
        crc.update(b);
      }
      return b;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
      int n = delegate.read(buf, off, len);
      if (n > 0) {
        crc.update(buf, off, n);
      }
      return n;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
