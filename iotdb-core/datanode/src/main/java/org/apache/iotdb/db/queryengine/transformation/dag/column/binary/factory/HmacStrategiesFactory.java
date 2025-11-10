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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary.factory;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.strategies.HmacStrategy;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public final class HmacStrategiesFactory {

  private HmacStrategiesFactory() {}

  // --- variable keys HmacStrategy instances ---
  // keypoint: create the HashFunction with the variable key every time
  public static final HmacStrategy HMAC_MD5 =
      (data, key) -> Hashing.hmacMd5(key).hashBytes(data).asBytes();

  public static final HmacStrategy HMAC_SHA1 =
      (data, key) -> Hashing.hmacSha1(key).hashBytes(data).asBytes();

  public static final HmacStrategy HMAC_SHA256 =
      (data, key) -> Hashing.hmacSha256(key).hashBytes(data).asBytes();

  public static final HmacStrategy HMAC_SHA512 =
      (data, key) -> Hashing.hmacSha512(key).hashBytes(data).asBytes();

  // ---static factory methods for creating HmacStrategy with a constant key---
  // keypoint: precompute the HashFunction with the constant key
  // the ignoredKey parameter is ignored because we use the constantKey

  public static HmacStrategy createConstantKeyHmacMd5Strategy(byte[] constantKey) {
    final HashFunction hashFunction;
    try {
      hashFunction = Hashing.hmacMd5(constantKey);
    } catch (IllegalArgumentException e) {
      throw new SemanticException(
          "Failed to execute function hmac_md5 due to an invalid input format, the empty key is not allowed in HMAC operation.");
    }
    return (data, ignoredKey) -> hashFunction.hashBytes(data).asBytes();
  }

  public static HmacStrategy createConstantKeyHmacSha1Strategy(byte[] constantKey) {
    final HashFunction hashFunction;
    try {
      hashFunction = Hashing.hmacSha1(constantKey);
    } catch (IllegalArgumentException e) {
      throw new SemanticException(
          "Failed to execute function hmac_sha1 due to an invalid input format, the empty key is not allowed in HMAC operation.");
    }
    return (data, ignoredKey) -> hashFunction.hashBytes(data).asBytes();
  }

  public static HmacStrategy createConstantKeyHmacSha256Strategy(byte[] constantKey) {
    final HashFunction hashFunction;
    try {
      hashFunction = Hashing.hmacSha256(constantKey);
    } catch (IllegalArgumentException e) {
      throw new SemanticException(
          "Failed to execute function hmac_sha256 due to an invalid input format, the empty key is not allowed in HMAC operation.");
    }
    return (data, ignoredKey) -> hashFunction.hashBytes(data).asBytes();
  }

  public static HmacStrategy createConstantKeyHmacSha512Strategy(byte[] constantKey) {
    final HashFunction hashFunction;
    try {
      hashFunction = Hashing.hmacSha512(constantKey);
    } catch (IllegalArgumentException e) {
      throw new SemanticException(
          "Failed to execute function hmac_sha512 due to an invalid input format, the empty key is not allowed in HMAC operation.");
    }
    return (data, ignoredKey) -> hashFunction.hashBytes(data).asBytes();
  }
}
