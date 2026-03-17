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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import net.minidev.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OpenIdAuthorizerTest {

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private PrivateKey privateKey;
  private JSONObject publicJwk;

  @Before
  public void setUp() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    privateKey = keyPair.getPrivate();
    publicJwk =
        new JSONObject(
            new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                .keyUse(KeyUse.SIGNATURE)
                .keyID("datanode-openid-test-key")
                .build()
                .toJSONObject());
  }

  @After
  public void tearDown() throws IOException {}

  @Test
  public void loginWithJWT() throws Exception {
    String jwt = createJwt(false);
    OpenIdAuthorizer authorizer = new OpenIdAuthorizer(publicJwk);
    assertTrue(authorizer.login(jwt, null, false));
  }

  @Test
  public void isAdmin_hasAccess() throws Exception {
    String jwt = createJwt(true);
    OpenIdAuthorizer authorizer = new OpenIdAuthorizer(publicJwk);
    assertTrue(authorizer.isAdmin(jwt));
  }

  @Test
  public void isAdmin_noAdminClaim() throws Exception {
    String jwt = createJwt(false);
    OpenIdAuthorizer authorizer = new OpenIdAuthorizer(publicJwk);
    assertFalse(authorizer.isAdmin(jwt));
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
        createSignedToken(
            keyPair.getPrivate(),
            new JWTClaimsSet.Builder()
                .subject("attacker")
                .expirationTime(Date.from(Instant.now().minusSeconds(3600)))
                .claim(
                    "realm_access",
                    Collections.singletonMap(
                        "roles",
                        Collections.singletonList(OpenIdAuthorizer.IOTDB_ADMIN_ROLE_NAME))));

    assertFalse(authorizer.login(expiredToken, "", false));
    assertFalse(authorizer.isAdmin(expiredToken));
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
          createSignedToken(
              keyPair.getPrivate(),
              new JWTClaimsSet.Builder()
                  .subject("attacker")
                  .issuer("https://evil.example/issuer")
                  .audience("iotdb")
                  .expirationTime(Date.from(Instant.now().plusSeconds(3600)))
                  .claim(
                      "realm_access",
                      Collections.singletonMap(
                          "roles",
                          Collections.singletonList(OpenIdAuthorizer.IOTDB_ADMIN_ROLE_NAME))));

      assertFalse(authorizer.login(token, "", false));
      assertFalse(authorizer.isAdmin(token));
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
          createSignedToken(
              keyPair.getPrivate(),
              new JWTClaimsSet.Builder()
                  .subject("attacker")
                  .issuer(issuer)
                  .audience("unrelated-client")
                  .expirationTime(Date.from(Instant.now().plusSeconds(3600)))
                  .claim(
                      "realm_access",
                      Collections.singletonMap(
                          "roles",
                          Collections.singletonList(OpenIdAuthorizer.IOTDB_ADMIN_ROLE_NAME))));

      assertFalse(authorizer.login(token, "", false));
      assertFalse(authorizer.isAdmin(token));
    } finally {
      server.stop(0);
    }
  }

  private KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    return keyPairGenerator.generateKeyPair();
  }

  private String createSignedToken(PrivateKey privateKey, JWTClaimsSet.Builder claimsBuilder)
      throws JOSEException {
    SignedJWT signedJwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claimsBuilder.build());
    signedJwt.sign(new RSASSASigner(privateKey));
    return signedJwt.serialize();
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

  private String createJwt(boolean hasAdminRole) throws JOSEException {
    JWTClaimsSet.Builder claimsBuilder =
        new JWTClaimsSet.Builder()
            .subject("datanode-test-user")
            .expirationTime(Date.from(Instant.now().plusSeconds(3600)))
            .claim(
                "realm_access",
                Collections.singletonMap(
                    "roles",
                    hasAdminRole
                        ? Collections.singletonList(OpenIdAuthorizer.IOTDB_ADMIN_ROLE_NAME)
                        : Collections.singletonList("offline_access")));
    SignedJWT signedJwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claimsBuilder.build());
    signedJwt.sign(new RSASSASigner(privateKey));
    return signedJwt.serialize();
  }
}
