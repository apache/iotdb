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
package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.OpenIdAuthorizer;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.util.JSONObjectUtils;
import net.minidev.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
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

  private static final String OPEN_ID_PUBLIC_JWK =
      "{\"kty\":\"RSA\",\"x5t#S256\":\"TZFbbj6HsRU28HYvrcVnDs03KreV3DE24-Cxb9EPdS4\",\"e\":\"AQAB\",\"use\":\"sig\",\"x5t\":\"l_N2UlC_a624iu5eYFypnB1Wr20\",\"kid\":\"q1-Wm0ozQ5O0mQH8-SJap2ZcN4MmucWwnQWKYxZJ4ow\",\"x5c\":[\"MIICmTCCAYECBgFyRdXW2DANBgkqhkiG9w0BAQsFADAQMQ4wDAYDVQQDDAVJb1REQjAeFw0yMDA1MjQwODM3MjJaFw0zMDA1MjQwODM5MDJaMBAxDjAMBgNVBAMMBUlvVERCMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAozDCZTVc9946VvhZ6E\\/OP8Yx6tJe0i9GR2Q9jR9S3jQoo0haT\\/P1b\\/zvQK52qA1xj6tBVg64xl3+LUxtCvh3HfAM5Q3PeSa0e2MkZaKCt335lKnKCSuaQGYoHULmg\\/FDOgCA0wJYOonGGJkgWmkzSAzdnHmBATosTl0XkBXHTdFOq5HaKw+bfghYp5097Gkl\\/Dp4sixVjIWLTh5l9diy4D\\/XKxadGumPCmTOS5E7y92jiHE64XFe1Q7v1qD+qKJKFvamAMIFPGBKegIajt42IcOIcIaJZnM1lBZApq1a\\/E6oL24QnP\\/j2e9coseDtGNywaADQdO8PaJadH\\/BV4aPCwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBX4rsWPIAwgSK6BEZmtEkh\\/FMfZtkvCFANpwkCX5Pph8yuk\\/8xrvx30yb4fIgqsxxQk6H+Q1qptm1cXs0tNu1yft+t+B2VuVjrWtkCkV0hAy6eZcdW411Pt523pHoOTxg6ehQd5DsvCIlsvWo83ePTKME+092vfs3irfQcRzc5xINdpopSvZlZuQ83tNEJY8gWvspQZr+uj8AP2x6w0BOrPJIiLlV+peNJuD3UgJKlSfOueKbKeM1kIVOG\\/a2AoEkBgqktnaIWzkXbk475\\/0xfGegsSZrxGR3\\/SA3jegS0sHFCY7\\/Ie\\/UvDgqMjd207oT64jxEGrd4mObxOx7aS0tp\"],\"alg\":\"RS256\",\"n\":\"ozDCZTVc9946VvhZ6E_OP8Yx6tJe0i9GR2Q9jR9S3jQoo0haT_P1b_zvQK52qA1xj6tBVg64xl3-LUxtCvh3HfAM5Q3PeSa0e2MkZaKCt335lKnKCSuaQGYoHULmg_FDOgCA0wJYOonGGJkgWmkzSAzdnHmBATosTl0XkBXHTdFOq5HaKw-bfghYp5097Gkl_Dp4sixVjIWLTh5l9diy4D_XKxadGumPCmTOS5E7y92jiHE64XFe1Q7v1qD-qKJKFvamAMIFPGBKegIajt42IcOIcIaJZnM1lBZApq1a_E6oL24QnP_j2e9coseDtGNywaADQdO8PaJadH_BV4aPCw\"}";
  private static CommonConfig config;
  private PrivateKey privateKey;
  private JSONObject publicJwk;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    config = CommonDescriptor.getInstance().getConfig();
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
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void loginWithJWT() throws Exception {
    String jwt = createJwt(false);

    OpenIdAuthorizer authorizer = new OpenIdAuthorizer(publicJwk);
    boolean login = authorizer.login(jwt, null, false);

    assertTrue(login);
  }

  @Test
  public void isAdmin_hasAccess() throws Exception {
    String jwt = createJwt(true);

    OpenIdAuthorizer authorizer = new OpenIdAuthorizer(publicJwk);
    boolean admin = authorizer.isAdmin(jwt);

    assertTrue(admin);
  }

  @Test
  public void isAdmin_noAdminClaim() throws Exception {
    String jwt = createJwt(false);

    OpenIdAuthorizer authorizer = new OpenIdAuthorizer(publicJwk);
    boolean admin = authorizer.isAdmin(jwt);

    assertFalse(admin);
  }

  /** Can be run manually as long as the site below is active... */
  @Test
  @Ignore("We have to find a way to test this against a defined OIDC Provider")
  public void fetchMetadata()
      throws ParseException, IOException, URISyntaxException, AuthException {
    OpenIdAuthorizer openIdAuthorizer =
        new OpenIdAuthorizer("https://auth.demo.pragmaticindustries.de/auth/realms/IoTDB/");
    boolean login =
        openIdAuthorizer.login(
            "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJxMS1XbTBvelE1TzBtUUg4LVNKYXAyWmNONE1tdWNXd25RV0tZeFpKNG93In0.eyJleHAiOjE1OTAzMTcxNzYsImlhdCI6MTU5MDMxNjg3NiwianRpIjoiY2MyNWQ3MDAtYjc5NC00OTA4LTg0OGUtOTRhNzYzNmM5YzQxIiwiaXNzIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6Ijg2YWRmNGIzLWE4ZTUtNDc1NC1iNWEwLTQ4OGI0OWY0M2VkMiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImlvdGRiIiwic2Vzc2lvbl9zdGF0ZSI6Ijk0ZmI5NGZjLTg3YTMtNDg4Ny04M2Q3LWE5MmQ1MzMzOTMzMCIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoiZW1haWwgcHJvZmlsZSIsImNsaWVudEhvc3QiOiIxOTIuMTY4LjE2OS4yMSIsImNsaWVudElkIjoiaW90ZGIiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1pb3RkYiIsImNsaWVudEFkZHJlc3MiOiIxOTIuMTY4LjE2OS4yMSJ9.GxQFltm1PrZzVL7rR6K-GpQINFLymjqAxxoDt_DGfQEMt61M6ebmx2oHiP_3G0HDSl7sbamajQbbRrfyTg--emBC2wfhdZ7v_7O0qWC60Yd8cWZ9qxwqwTFKYb8a0Z6_TeH9-vUmsy6kp2BfJZXq3mSy0My21VGUAXRmWTbghiM4RFoHKjAZVhsPHWelFmtLftYPdOGxv-7c9iUOVh_W-nOcCNRJpYY7BEjUYN24TsjvCEwWDQWD9E29LMYfA6LNeG0KdL9Jvqad4bc2FTJn9TaCnJMCiAJ7wEEiotqhXn70uEBWYxGXIVlm3vn3MDe3pTKA2TZy7U5xcrE7S8aGMg",
            "",
            false);
    assertTrue(login);
    config.setOpenIdProviderUrl("https://auth.demo.pragmaticindustries.de/auth/realms/IoTDB/");
    OpenIdAuthorizer openIdAuthorizer1 = new OpenIdAuthorizer();
    login =
        openIdAuthorizer1.login(
            "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJxMS1XbTBvelE1TzBtUUg4LVNKYXAyWmNONE1tdWNXd25RV0tZeFpKNG93In0.eyJleHAiOjE1OTAzMTcxNzYsImlhdCI6MTU5MDMxNjg3NiwianRpIjoiY2MyNWQ3MDAtYjc5NC00OTA4LTg0OGUtOTRhNzYzNmM5YzQxIiwiaXNzIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6Ijg2YWRmNGIzLWE4ZTUtNDc1NC1iNWEwLTQ4OGI0OWY0M2VkMiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImlvdGRiIiwic2Vzc2lvbl9zdGF0ZSI6Ijk0ZmI5NGZjLTg3YTMtNDg4Ny04M2Q3LWE5MmQ1MzMzOTMzMCIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoiZW1haWwgcHJvZmlsZSIsImNsaWVudEhvc3QiOiIxOTIuMTY4LjE2OS4yMSIsImNsaWVudElkIjoiaW90ZGIiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1pb3RkYiIsImNsaWVudEFkZHJlc3MiOiIxOTIuMTY4LjE2OS4yMSJ9.GxQFltm1PrZzVL7rR6K-GpQINFLymjqAxxoDt_DGfQEMt61M6ebmx2oHiP_3G0HDSl7sbamajQbbRrfyTg--emBC2wfhdZ7v_7O0qWC60Yd8cWZ9qxwqwTFKYb8a0Z6_TeH9-vUmsy6kp2BfJZXq3mSy0My21VGUAXRmWTbghiM4RFoHKjAZVhsPHWelFmtLftYPdOGxv-7c9iUOVh_W-nOcCNRJpYY7BEjUYN24TsjvCEwWDQWD9E29LMYfA6LNeG0KdL9Jvqad4bc2FTJn9TaCnJMCiAJ7wEEiotqhXn70uEBWYxGXIVlm3vn3MDe3pTKA2TZy7U5xcrE7S8aGMg",
            "",
            false);
    assertTrue(login);
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
