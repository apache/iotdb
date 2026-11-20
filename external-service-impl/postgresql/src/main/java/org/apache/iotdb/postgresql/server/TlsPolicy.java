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

package org.apache.iotdb.postgresql.server;

import javax.net.ssl.SSLEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The TLS settings of the PostgreSQL endpoint: TLS 1.2 and 1.3 with AEAD cipher suites only. Older
 * protocols and CBC suites are never enabled, even if the JVM still offers them.
 */
final class TlsPolicy {

  static final List<String> PROTOCOLS =
      Collections.unmodifiableList(Arrays.asList("TLSv1.3", "TLSv1.2"));

  static final List<String> CIPHER_SUITES =
      Collections.unmodifiableList(
          Arrays.asList(
              "TLS_AES_256_GCM_SHA384",
              "TLS_AES_128_GCM_SHA256",
              "TLS_CHACHA20_POLY1305_SHA256",
              "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
              "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
              "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
              "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
              "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
              "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"));

  private TlsPolicy() {}

  /** Restricts the engine to the allowed protocols and suites; never falls back to weaker ones. */
  static void restrict(SSLEngine engine) {
    String[] protocols = supported(PROTOCOLS, engine.getSupportedProtocols());
    String[] suites = supported(CIPHER_SUITES, engine.getSupportedCipherSuites());
    if (protocols.length == 0 || suites.length == 0) {
      throw new IllegalStateException(
          "The JVM supports none of the allowed TLS protocols " + PROTOCOLS + " or cipher suites");
    }
    engine.setEnabledProtocols(protocols);
    engine.setEnabledCipherSuites(suites);
  }

  private static String[] supported(List<String> allowed, String[] supportedByJvm) {
    Set<String> supported = new HashSet<>(Arrays.asList(supportedByJvm));
    List<String> result = new ArrayList<>();
    for (String value : allowed) {
      if (supported.contains(value)) {
        result.add(value);
      }
    }
    return result.toArray(new String[0]);
  }
}
