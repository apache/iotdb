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

package org.apache.iotdb.commons.auth.authorizer;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import io.jsonwebtoken.Jwts;
import net.minidev.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

public class OpenIdAuthorizerTest {

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();

  private String originalUserFolder;
  private String originalRoleFolder;
  private Path baseDir;

  @Before
  public void setUp() throws IOException {
    originalUserFolder = config.getUserFolder();
    originalRoleFolder = config.getRoleFolder();

    baseDir = Files.createTempDirectory("openid-authorizer-test-");
    config.setUserFolder(Files.createDirectories(baseDir.resolve("users")).toString());
    config.setRoleFolder(Files.createDirectories(baseDir.resolve("roles")).toString());
  }

  @After
  public void tearDown() throws IOException {
    config.setUserFolder(originalUserFolder);
    config.setRoleFolder(originalRoleFolder);

    if (baseDir != null) {
      try (java.util.stream.Stream<Path> stream = Files.walk(baseDir)) {
        stream.sorted(Comparator.reverseOrder()).forEach(this::deleteIfExists);
      }
    }
  }

  @Test
  public void testExpiredTokenRejectedByLoginAndIsAdmin() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    KeyPair keyPair = keyPairGenerator.generateKeyPair();

    JSONObject jwk =
        new JSONObject(
            new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                .privateKey(keyPair.getPrivate())
                .keyUse(KeyUse.SIGNATURE)
                .keyID("expired-token-test-key")
                .build()
                .toJSONObject());

    OpenIdAuthorizer authorizer = new OpenIdAuthorizer(jwk);
    String expiredToken =
        Jwts.builder()
            .subject("attacker")
            .expiration(Date.from(Instant.now().minusSeconds(3600)))
            .claim(
                "realm_access",
                Collections.singletonMap(
                    "roles", Collections.singletonList(OpenIdAuthorizer.IOTDB_ADMIN_ROLE_NAME)))
            .signWith(keyPair.getPrivate(), Jwts.SIG.RS256)
            .compact();

    Assert.assertFalse(authorizer.login(expiredToken, "", false));
    Assert.assertFalse(authorizer.isAdmin(expiredToken));
  }

  private void deleteIfExists(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete test path " + path, e);
    }
  }
}
