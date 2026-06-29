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

package org.apache.iotdb.commons.security.encrypt;

public interface AsymmetricEncrypt {

  /**
   * Defines cryptographic hash algorithms supported by the system. Each enum constant represents a
   * specific message digest algorithm compatible with {@link java.security.MessageDigest}.
   */
  enum DigestAlgorithm {
    MD5("MD5"),
    SHA_256("SHA-256");

    private final String algorithmName;

    DigestAlgorithm(String algorithmName) {
      this.algorithmName = algorithmName;
    }

    public String getAlgorithmName() {
      return this.algorithmName;
    }
  }

  /**
   * init some providerParameter
   *
   * @param providerParameters encrypt need some parameters
   */
  void init(String providerParameters);

  /**
   * encrypt a password
   *
   * @param originPassword password to be crypt
   * @return encrypt password
   */
  String encrypt(String originPassword, DigestAlgorithm digestAlgorithm);

  /**
   * validate originPassword and encryptPassword
   *
   * @param originPassword origin password
   * @param encryptPassword encrypt password
   * @return true if validate success
   */
  boolean validate(String originPassword, String encryptPassword, DigestAlgorithm digestAlgorithm);
}
