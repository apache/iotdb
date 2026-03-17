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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;
import net.minidev.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
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
  private String originalOpenIdAudience;
  private Path baseDir;

  @Before
  public void setUp() throws IOException {
    originalUserFolder = config.getUserFolder();
    originalRoleFolder = config.getRoleFolder();
    originalOpenIdAudience = config.getOpenIdAudience();

    baseDir = Files.createTempDirectory("openid-authorizer-test-");
    config.setUserFolder(Files.createDirectories(baseDir.resolve("users")).toString());
    config.setRoleFolder(Files.createDirectories(baseDir.resolve("roles")).toString());
  }

  @After
  public void tearDown() throws IOException {
    config.setUserFolder(originalUserFolder);
    config.setRoleFolder(originalRoleFolder);
    config.setOpenIdAudience(originalOpenIdAudience);

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

  @Test
  public void testWrongIssuerRejected() throws Exception {
    config.setOpenIdAudience("iotdb");
    KeyPair keyPair = generateKeyPair();
    HttpServer server = startProviderServer(keyPair);
    String issuer = "http://127.0.0.1:" + server.getAddress().getPort() + "/";

    try {
      OpenIdAuthorizer authorizer = new OpenIdAuthorizer(issuer);
      String token =
          Jwts.builder()
              .subject("attacker")
              .issuer("https://evil.example/issuer")
              .claim("aud", "iotdb")
              .expiration(Date.from(Instant.now().plusSeconds(3600)))
              .claim(
                  "realm_access",
                  Collections.singletonMap(
                      "roles", Collections.singletonList(OpenIdAuthorizer.IOTDB_ADMIN_ROLE_NAME)))
              .signWith(keyPair.getPrivate(), Jwts.SIG.RS256)
              .compact();

      Assert.assertFalse(authorizer.login(token, "", false));
      Assert.assertFalse(authorizer.isAdmin(token));
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testWrongAudienceRejected() throws Exception {
    config.setOpenIdAudience("iotdb");
    KeyPair keyPair = generateKeyPair();
    HttpServer server = startProviderServer(keyPair);
    String issuer = "http://127.0.0.1:" + server.getAddress().getPort() + "/";

    try {
      OpenIdAuthorizer authorizer = new OpenIdAuthorizer(issuer);
      String token =
          Jwts.builder()
              .subject("attacker")
              .issuer(issuer)
              .claim("aud", "unrelated-client")
              .expiration(Date.from(Instant.now().plusSeconds(3600)))
              .claim(
                  "realm_access",
                  Collections.singletonMap(
                      "roles", Collections.singletonList(OpenIdAuthorizer.IOTDB_ADMIN_ROLE_NAME)))
              .signWith(keyPair.getPrivate(), Jwts.SIG.RS256)
              .compact();

      Assert.assertFalse(authorizer.login(token, "", false));
      Assert.assertFalse(authorizer.isAdmin(token));
    } finally {
      server.stop(0);
    }
  }

  private KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    return keyPairGenerator.generateKeyPair();
  }

  private HttpServer startProviderServer(KeyPair keyPair) throws Exception {
    JSONObject publicJwk =
        new JSONObject(
            new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                .keyUse(KeyUse.SIGNATURE)
                .keyID("openid-provider-test-key")
                .build()
                .toJSONObject());

    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    String issuer = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    String metadata =
        "{"
            + "\"issuer\":\""
            + issuer
            + "\","
            + "\"jwks_uri\":\""
            + issuer
            + "jwks.json\","
            + "\"subject_types_supported\":[\"public\"],"
            + "\"response_types_supported\":[\"code\"],"
            + "\"id_token_signing_alg_values_supported\":[\"RS256\"]"
            + "}";
    String jwks = "{\"keys\":[" + publicJwk.toJSONString() + "]}";

    server.createContext(
        "/.well-known/openid-configuration", exchange -> writeJson(exchange, metadata));
    server.createContext("/jwks.json", exchange -> writeJson(exchange, jwks));
    server.start();
    return server;
  }

  private void writeJson(HttpExchange exchange, String json) throws IOException {
    byte[] response = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, response.length);
    try (OutputStream outputStream = exchange.getResponseBody()) {
      outputStream.write(response);
    }
  }

  private void deleteIfExists(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete test path " + path, e);
    }
  }
}
