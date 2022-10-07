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
package org.apache.iotdb.tsfile.compress.arithmetic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Compression application using adaptive arithmetic coding.
 *
 * <p>Usage: java AdaptiveArithmeticCompress InputFile OutputFile
 *
 * <p>Then use the corresponding "AdaptiveArithmeticDecompress" application to recreate the original
 * input file.
 *
 * <p>Note that the application starts with a flat frequency table of 257 symbols (all set to a
 * frequency of 1), and updates it after each byte encoded. The corresponding decompressor program
 * also starts with a flat frequency table and updates it after each byte decoded. It is by design
 * that the compressor and decompressor have synchronized states, so that the data can be
 * decompressed properly.
 */
public class AdaptiveArithmeticCompress {

  public static byte[] compress(byte[] input) throws IOException {
    InputStream in = new ByteArrayInputStream(input);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BitOutputStream outStream = new BitOutputStream(out);
    compress(in, outStream);
    outStream.close();
    return out.toByteArray();
  }

  public static byte[] decompress(byte[] input) throws IOException {

    BitInputStream in = new BitInputStream(new ByteArrayInputStream(input));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    AdaptiveArithmeticDecompress.decompress(in, out);
    out.close();
    return out.toByteArray();
  }

  // To allow unit testing, this method is package-private instead of private.
  static void compress(InputStream in, BitOutputStream out) throws IOException {
    FlatFrequencyTable initFreqs = new FlatFrequencyTable(257);
    FrequencyTable freqs = new SimpleFrequencyTable(initFreqs);
    ArithmeticEncoder enc = new ArithmeticEncoder(32, out);
    while (true) {
      // Read and encode one byte
      int symbol = in.read();
      if (symbol == -1) {
        break;
      }
      enc.write(freqs, symbol);
      freqs.increment(symbol);
    }
    enc.write(freqs, 256); // EOF
    enc.finish(); // Flush remaining code bits
  }
}
