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

package com.timecho.iotdb.commons.commission.complete;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class HmacProtocol {
  private static final String HMAC_ALGO = "HmacSHA256";
  private static final byte[] KEY = "Y5ghi654F5eHA4c5Jgc+c9vTHE2DEwmJfKPAm6EZ/Qc=".getBytes();
  public static final int SIGN_LENGTH = 8;

  private static byte[] hmacSha256Truncated(byte[]... dataChunks) throws Exception {
    Mac sha256HMAC = Mac.getInstance(HMAC_ALGO);
    SecretKeySpec secretKey = new SecretKeySpec(KEY, HMAC_ALGO);
    sha256HMAC.init(secretKey);

    for (byte[] chunk : dataChunks) {
      sha256HMAC.update(chunk);
    }

    byte[] fullHmac = sha256HMAC.doFinal();

    // Truncate to the first 8 bytes
    byte[] tag = new byte[8];
    System.arraycopy(fullHmac, 0, tag, 0, 8);
    return tag;
  }

  public static byte[] calculateTag(byte[] data, String s9) throws Exception {
    return hmacSha256Truncated(data, s9.getBytes());
  }

  public static byte[] calculateTag(byte[] data) throws Exception {
    return hmacSha256Truncated(data);
  }
}
