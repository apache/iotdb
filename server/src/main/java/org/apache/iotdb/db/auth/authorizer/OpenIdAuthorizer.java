/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.auth.authorizer;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.role.IRoleManager;
import org.apache.iotdb.db.auth.role.LocalFileRoleManager;
import org.apache.iotdb.db.auth.user.IUserManager;
import org.apache.iotdb.db.auth.user.LocalFileUserManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Uses an OpenID Connect provider for Authorization / Authentication.
 */
public class OpenIdAuthorizer extends BasicAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(OpenIdAuthorizer.class);

    private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    private final String secret;

    public OpenIdAuthorizer() throws AuthException {
        this(config.getOpenIdSecret());
    }

    OpenIdAuthorizer(String secret) throws AuthException {
        super(new LocalFileUserManager(config.getSystemDir() + File.separator + "users"),
                new LocalFileRoleManager(config.getSystemDir() + File.separator + "roles"));
        if (secret == null) {
            throw new IllegalArgumentException("OpenID Secret is null which is not allowed!");
        }
        this.secret = secret;
    }

    /**
     * function for getting the instance of the local file authorizer.
     */
    public static OpenIdAuthorizer getInstance() throws AuthException {
        if (OpenIdAuthorizer.InstanceHolder.instance == null) {
            throw new AuthException("Authorizer uninitialized");
        }
        return OpenIdAuthorizer.InstanceHolder.instance;
    }

    private static class InstanceHolder {
        private static OpenIdAuthorizer instance;

        static {
            // Only for testing here!
            IoTDBDescriptor.getInstance().getConfig().setOpenIdSecret("111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
            try {
                instance = new OpenIdAuthorizer();
            } catch (AuthException e) {
                logger.error("Authorizer initialization failed due to ", e);
                instance = null;
            }
        }
    }

    @Override
    public boolean login(String token, String password) throws AuthException {
        if (password != null && !password.isEmpty()) {
            logger.error("JWT Login failed as a non-empty Password was given username (token): {}, password: {}", token, password);
            return false;
        }
        if (token == null || token.isEmpty()) {
            logger.error("JWT Login failed as a Username (token) was empty!");
            return false;
        }
        //This line will throw an exception if it is not a signed JWS (as expected)
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
        if (!super.listAllUsers().contains(claims.getId())) {
            logger.info("User {} logs in for first time, storing it locally!", claims.getId());
            super.createUser(claims.getSubject(), "UNUSED_PASSWORT");
        }
        return true;
    }

    private Claims validateToken(String token) throws JwtException {
        return Jwts
                .parser()
                // Basically ignore the Expiration Date, if there is any???
                .setAllowedClockSkewSeconds(Long.MAX_VALUE / 1000)
                // .setSigningKey(DatatypeConverter.parseBase64Binary(secret))
                .setSigningKey(secret.getBytes())
                .parseClaimsJws(token)
                .getBody();
    }

    @Override
    public void createUser(String username, String password) throws AuthException {
        throw new UnsupportedOperationException("This operation is not supported for JWT Auth Provider!");
    }

    @Override
    public void deleteUser(String username) throws AuthException {
        throw new UnsupportedOperationException("This operation is not supported for JWT Auth Provider!");
    }

    @Override
    boolean isAdmin(String token) {
        Claims claims;
        try {
            claims = validateToken(token);
        } catch (JwtException e) {
            logger.warn("Unable to validate token {}!", token, e);
            return false;
        }
        if (!(claims.get("IOTDB_ADMIN") instanceof Boolean) || !claims.get("IOTDB_ADMIN", Boolean.class)) {
            logger.warn("Given Token has no admin rights, is custom claim IOTDB_ADMIN set to true?");
            return false;
        }
        return true;
    }

//    @Override
//    public void grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException {
//        if (isAdmin(username)) {
//            throw new AuthException("Given Token has no Admin privileges!");
//        }
//        // Yes, you are Admin! Gratz!
//        // Do something here...
//        super.grantPrivilegeToUser(username, path, privilegeId);
//    }
//    @Override
//    public void revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void createRole(String roleName) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void deleteRole(String roleName) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void grantPrivilegeToRole(String roleName, String path, int privilegeId) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void revokePrivilegeFromRole(String roleName, String path, int privilegeId) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void grantRoleToUser(String roleName, String username) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void revokeRoleFromUser(String roleName, String username) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public Set<Integer> getPrivileges(String username, String path) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }

    @Override
    public void updateUserPassword(String username, String newPassword) throws AuthException {
        throw new UnsupportedOperationException("This operation is not supported for JWT Auth Provider!");
    }
//
//    @Override
//    public boolean checkUserPrivileges(String username, String path, int privilegeId) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void reset() throws AuthException {
//        // Do nothing
//        super.reset();
//    }
//
//    @Override
//    public List<String> listAllUsers() {
//        // Unsure if we list all "known" users or just throw this exception??
//        throw new UnsupportedOperationException("This operation is not supported for JWT Auth Provider!");
//    }
//
//    @Override
//    public List<String> listAllRoles() {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public Role getRole(String roleName) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public User getUser(String username) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public boolean isUserUseWaterMark(String userName) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
//
//    @Override
//    public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
//        throw new NotImplementedException("Not yet implemented!");
//    }
}
