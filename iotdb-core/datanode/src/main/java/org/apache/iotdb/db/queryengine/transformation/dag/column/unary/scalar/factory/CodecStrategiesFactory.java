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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.factory;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.inteface.CodecStrategy;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util.HexUtils;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util.SpookyHashV2Utils;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.tsfile.common.conf.TSFileConfig;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * for byte[] to byte[] codec transformations, including encoding/decoding and hashing functions for
 * decoding errors, it has been wrapped into SemanticException and will be handled in the upper
 * layer
 */
public final class CodecStrategiesFactory {

  private static final BaseEncoding GUAVA_BASE32_ENCODING = BaseEncoding.base32();

  // --- Base64 ---
  public static final CodecStrategy TO_BASE64 = Base64.getEncoder()::encode;
  public static final CodecStrategy FROM_BASE64 =
      (input) -> {
        try {
          return Base64.getDecoder().decode(input);
        } catch (IllegalArgumentException e) {
          // wrap the specific exception in dependency into a general one for uniform handling in
          // upper layer
          throw new SemanticException("decode base64 error");
        }
      };

  // --- Base64URL ---
  public static final CodecStrategy TO_BASE64URL = Base64.getUrlEncoder().withoutPadding()::encode;
  public static final CodecStrategy FROM_BASE64URL =
      (input) -> {
        try {
          return Base64.getUrlDecoder().decode(input);
        } catch (IllegalArgumentException e) {
          throw new SemanticException("decode base64url error");
        }
      };

  // --- Base32 ---
  public static final CodecStrategy TO_BASE32 =
      (data) -> GUAVA_BASE32_ENCODING.encode(data).getBytes(TSFileConfig.STRING_CHARSET);
  public static final CodecStrategy FROM_BASE32 =
      (input) -> {
        try {
          return GUAVA_BASE32_ENCODING.decode(new String(input, TSFileConfig.STRING_CHARSET));
        } catch (IllegalArgumentException e) {
          throw new SemanticException("decode base32 error");
        }
      };

  // --- Hashing Function Strategies ---
  public static final CodecStrategy SHA256 = input -> Hashing.sha256().hashBytes(input).asBytes();
  public static final CodecStrategy SHA512 = input -> Hashing.sha512().hashBytes(input).asBytes();
  public static final CodecStrategy SHA1 = input -> Hashing.sha1().hashBytes(input).asBytes();
  public static final CodecStrategy MD5 = input -> Hashing.md5().hashBytes(input).asBytes();
  public static final CodecStrategy XXHASH64 =
      (input) ->
          ByteBuffer.allocate(8)
              .putLong(XXHashFactory.fastestInstance().hash64().hash(input, 0, input.length, 0L))
              .array();

  public static final CodecStrategy MURMUR3 =
      (input) -> Hashing.murmur3_128().hashBytes(input).asBytes();

  // --- Hex ---
  public static final CodecStrategy TO_HEX = HexUtils::toHex;
  public static final CodecStrategy FROM_HEX =
      input -> {
        try {
          return HexUtils.fromHex(input);
        } catch (IllegalArgumentException e) {
          throw new SemanticException("decode hex error");
        }
      };

  // --- Reverse ---
  /** Reverses the order of bytes in the input array. Suitable for BLOB type. */
  public static final CodecStrategy REVERSE_BYTES =
      (input) -> {
        int length = input.length;
        byte[] reversed = new byte[length];
        for (int i = 0; i < length; i++) {
          reversed[i] = input[length - 1 - i];
        }
        return reversed;
      };

  /**
   * Reverses the order of characters in the input string. Suitable for STRING and TEXT types. This
   * involves converting bytes to a String, reversing it, and converting back to bytes.
   */
  public static final CodecStrategy REVERSE_CHARS =
      (input) -> {
        String originalString = new String(input, TSFileConfig.STRING_CHARSET);
        String reversedString = new StringBuilder(originalString).reverse().toString();
        return reversedString.getBytes(TSFileConfig.STRING_CHARSET);
      };

  public static final CodecStrategy spooky_hash_v2_32 = SpookyHashV2Utils::hash32;

  public static final CodecStrategy spooky_hash_v2_64 = SpookyHashV2Utils::hash64;

  private CodecStrategiesFactory() {}
}
