/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.db.auth.AuthException;
import org.junit.Test;

import static org.junit.Assert.*;

public class OpenIdAuthorizerTest {

    @Test
    public void loginWithJWT() throws AuthException {
        String jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoiMTIzIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.PB603vtDyNkryxeLjomX1JQuSF2JHKXHyixzPBCA7tQ";
        String secret = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";

        OpenIdAuthorizer authorizer = new OpenIdAuthorizer(secret);
        boolean login = authorizer.login(jwt, null);

        assertTrue(login);
    }

    @Test
    public void isAdmin_hasAccess() throws AuthException {
        // IOTDB_ADMIN = true
        String jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoiMTIzIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJJT1REQl9BRE1JTiI6dHJ1ZX0.dxB417n9GFAGbwL7kyIvgenEBycjlJLZbB1I_GF0qd8";
        String secret = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";

        OpenIdAuthorizer authorizer = new OpenIdAuthorizer(secret);
        boolean admin = authorizer.isAdmin(jwt);

        assertTrue(admin);
    }

    @Test
    public void isAdmin_AdminClaimFalse() throws AuthException {
        // IOTDB_ADMIN = false
        String jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoiMTIzIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJJT1REQl9BRE1JTiI6ZmFsc2V9.80lCGEWhgW6YO55TFC98v_mj8ts0IcrBMb2drsxEpZ0";
        String secret = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";

        OpenIdAuthorizer authorizer = new OpenIdAuthorizer(secret);
        boolean admin = authorizer.isAdmin(jwt);

        assertFalse(admin);
    }

    @Test
    public void isAdmin_noAdminClaim() throws AuthException {
        // IOTDB_ADMIN = false
        String jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoiMTIzIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.PB603vtDyNkryxeLjomX1JQuSF2JHKXHyixzPBCA7tQ";
        String secret = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";

        OpenIdAuthorizer authorizer = new OpenIdAuthorizer(secret);
        boolean admin = authorizer.isAdmin(jwt);

        assertFalse(admin);
    }
}