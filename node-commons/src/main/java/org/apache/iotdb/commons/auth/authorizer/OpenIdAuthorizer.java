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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.role.LocalFileRoleManager;
import org.apache.iotdb.commons.auth.user.LocalFileUserManager;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.util.JSONObjectUtils;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

/** Uses an OpenID Connect provider for Authorization / Authentication. */
public class OpenIdAuthorizer extends BasicAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(OpenIdAuthorizer.class);
  public static final String IOTDB_ADMIN_ROLE_NAME = "iotdb_admin";
  public static final String OPENID_USER_PREFIX = "openid-";

  private static final CommonConfig config = CommonDescriptor.getInstance().getConfig();

  private final RSAPublicKey providerKey;

  /** Stores all claims to the respective user */
  private final Map<String, Claims> loggedClaims = new HashMap<>();

  public OpenIdAuthorizer() throws AuthException, ParseException, IOException, URISyntaxException {
    this(config.getOpenIdProviderUrl());
  }

  public OpenIdAuthorizer(JSONObject jwk) throws AuthException {
    super(
        new LocalFileUserManager(config.getUserFolder()),
        new LocalFileRoleManager(config.getRoleFolder()));
    try {
      providerKey = RSAKey.parse(jwk).toRSAPublicKey();
    } catch (java.text.ParseException | JOSEException e) {
      throw new AuthException(
          TSStatusCode.INIT_AUTH_ERROR, "Unable to get OIDC Provider Key from JWK " + jwk, e);
    }
    logger.info("Initialized with providerKey: {}", providerKey);
  }

  public OpenIdAuthorizer(String providerUrl)
      throws AuthException, URISyntaxException, ParseException, IOException {
    this(getJwkFromProvider(providerUrl));
  }

  private static JSONObject getJwkFromProvider(String providerUrl)
      throws URISyntaxException, IOException, ParseException, AuthException {
    if (providerUrl == null) {
      throw new IllegalArgumentException("OpenID Connect Provider URI must be given!");
    }

    // Fetch Metadata
    OIDCProviderMetadata providerMetadata = fetchMetadata(providerUrl);

    logger.debug("Using Provider Metadata: {}", providerMetadata);

    try {
      URL url = new URI(providerMetadata.getJWKSetURI().toString()).toURL();
      logger.debug("Using url {}", url);
      return getProviderRsaJwk(url.openStream());
    } catch (IOException e) {
      throw new AuthException(TSStatusCode.INIT_AUTH_ERROR, "Unable to start the Auth", e);
    }
  }

  private static JSONObject getProviderRsaJwk(InputStream is) throws ParseException {
    // Read all data from stream
    StringBuilder sb = new StringBuilder();
    try (Scanner scanner = new Scanner(is)) {
      while (scanner.hasNext()) {
        sb.append(scanner.next());
      }
    }

    // Parse the data as json
    String jsonString = sb.toString();
    JSONObject json = JSONObjectUtils.parse(jsonString);

    // Find the RSA signing key
    JSONArray keyList = (JSONArray) json.get("keys");
    for (Object key : keyList) {
      JSONObject k = (JSONObject) key;
      if ("sig".equals(k.get("use")) && "RSA".equals(k.get("kty"))) {
        return k;
      }
    }
    return null;
  }

  private static OIDCProviderMetadata fetchMetadata(String providerUrl)
      throws URISyntaxException, IOException, ParseException {
    URI issuerUri = new URI(providerUrl);
    URL providerConfigurationUrl = issuerUri.resolve(".well-known/openid-configuration").toURL();
    InputStream stream = providerConfigurationUrl.openStream();
    // Read all data from URL
    String providerInfo;
    try (java.util.Scanner s = new java.util.Scanner(stream)) {
      providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
    }
    return OIDCProviderMetadata.parse(providerInfo);
  }

  @Override
  public boolean login(String token, String password) throws AuthException {
    if (password != null && !password.isEmpty()) {
      logger.error(
          "JWT Login failed as a non-empty Password was given username (token): {}, password: {}",
          token,
          password);
      return false;
    }
    if (token == null || token.isEmpty()) {
      logger.error("JWT Login failed as a Username (token) was empty!");
      return false;
    }
    // This line will throw an exception if it is not a signed JWS (as expected)
    Claims claims;
    try {
      claims = validateToken(token);
    } catch (JwtException e) {
      logger.error("Unable to login the user wit jwt {}", password, e);
      return false;
    }
    logger.debug("JWT was validated successfully!");
    logger.debug("ID: {}", claims.getId());
    logger.debug("Subject: {}", claims.getSubject());
    logger.debug("Issuer: {}", claims.getIssuer());
    logger.debug("Expiration: {}", claims.getExpiration());
    // Create User if not exists
    String iotdbUsername = getUsername(claims);
    if (!super.listAllUsers().contains(iotdbUsername)) {
      logger.info("User {} logs in for first time, storing it locally!", iotdbUsername);
      // We give the user a random password so that no one could hijack them via local login
      super.createUser(iotdbUsername, UUID.randomUUID().toString());
    }
    // Always store claims and user
    this.loggedClaims.put(getUsername(claims), claims);
    return true;
  }

  public String getIoTDBUserName(String token) {
    Claims claims = validateToken(token);
    logger.debug("JWT was validated successfully!");
    logger.debug("ID: {}", claims.getId());
    logger.debug("Subject: {}", claims.getSubject());
    logger.debug("Issuer: {}", claims.getIssuer());
    logger.debug("Expiration: {}", claims.getExpiration());
    // Create User if not exists
    return getUsername(claims);
  }

  private Claims validateToken(String token) {
    return Jwts.parser()
        // Basically ignore the Expiration Date, if there is any???
        .setAllowedClockSkewSeconds(Long.MAX_VALUE / 1000)
        // .setSigningKey(DatatypeConverter.parseBase64Binary(secret))
        .setSigningKey(providerKey)
        .parseClaimsJws(token)
        .getBody();
  }

  private String getUsername(Claims claims) {
    return OPENID_USER_PREFIX + claims.getSubject();
  }

  @Override
  public void createUser(String username, String password) {
    throwUnsupportedOperationException();
  }

  private void throwUnsupportedOperationException() {
    throw new UnsupportedOperationException(
        "This operation is not supported for JWT Auth Provider!");
  }

  @Override
  public void deleteUser(String username) {
    throwUnsupportedOperationException();
  }

  /**
   * So not with the token!
   *
   * @param token Usually the JWT but could also be just the name of the user.
   * @return true if the user is an admin
   */
  @Override
  public boolean isAdmin(String token) {
    Claims claims;
    if (this.loggedClaims.containsKey(token)) {
      // This is a username!
      claims = this.loggedClaims.get(token);
    } else {
      // It's a token
      try {
        claims = validateToken(token);
      } catch (JwtException e) {
        logger.warn("Unable to validate token {}!", token, e);
        return false;
      }
    }
    // Get available roles (from keycloack)
    List<String> availableRoles =
        ((Map<String, List<String>>) claims.get("realm_access")).get("roles");
    if (!availableRoles.contains(IOTDB_ADMIN_ROLE_NAME)) {
      logger.warn(
          "Given Token has no admin rights, is there a ROLE with name {} in 'realm_access' role set?",
          IOTDB_ADMIN_ROLE_NAME);
      return false;
    }
    return true;
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    return isAdmin(username);
  }

  @Override
  public void updateUserPassword(String username, String newPassword) {
    throwUnsupportedOperationException();
  }
}
