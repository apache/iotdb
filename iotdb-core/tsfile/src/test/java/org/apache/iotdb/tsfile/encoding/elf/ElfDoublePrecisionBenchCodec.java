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

package org.apache.iotdb.tsfile.encoding.elf;

import org.apache.iotdb.tsfile.encoding.elf.compressor.ElfCompressor;
import org.apache.iotdb.tsfile.encoding.elf.decompressor.ElfDecompressor;

import java.util.Arrays;
import java.util.List;

/**
 * Double-precision ELF encode/decode for {@link
 * org.apache.iotdb.tsfile.encoding.DatasetEncoderCompressBinRoundtripBenchTest}.
 *
 * <p>Ported from {@code org.urbcomp.startdb.compress.elf} (elf@elf project), plain ELF only (not
 * ElfOnChimp / ElfOnGorilla).
 */
public final class ElfDoublePrecisionBenchCodec {

  private ElfDoublePrecisionBenchCodec() {}

  public static byte[] encode(double[] values) {
    int scratchBytes = Math.max(values.length * 16, 4096);
    ElfCompressor compressor = new ElfCompressor(scratchBytes);
    for (double v : values) {
      compressor.addValue(v);
    }
    compressor.close();
    return compressor.getBytes();
  }

  public static double[] decode(byte[] encoded) {
    ElfDecompressor decompressor = new ElfDecompressor(encoded);
    List<Double> decoded = decompressor.decompress();
    double[] out = new double[decoded.size()];
    for (int i = 0; i < decoded.size(); i++) {
      out[i] = decoded.get(i);
    }
    return out;
  }
}
