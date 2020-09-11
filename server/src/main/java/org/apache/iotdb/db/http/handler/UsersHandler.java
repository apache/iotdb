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
package org.apache.iotdb.db.http.handler;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.http.constant.HttpConstant;

public class UsersHandler extends Handler{

  public boolean userLogin(Map<String, List<String>> p) throws AuthException {
    List<String> usernameList = p.get(HttpConstant.USERNAME);
    List<String> passwordList = p.get(HttpConstant.PASSWORD);
    IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
    username = usernameList.get(0);
    return authorizer.login(usernameList.get(0), passwordList.get(0));
  }

  public boolean userLogout(Map<String, List<String>> p) throws AuthException {
    if(username == null) {
      throw new AuthException("you have already logout");
    }
    List<String> usernameList = p.get(HttpConstant.USERNAME);
    if(!usernameList.get(0).equals(username)) {
      throw new AuthException("wrong username");
    }
    username = null;
    return true;
  }
}
