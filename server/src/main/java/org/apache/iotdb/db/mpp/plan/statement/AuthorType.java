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
package org.apache.iotdb.db.mpp.plan.statement;

public enum AuthorType {
  CREATE_USER,
  CREATE_ROLE,
  DROP_USER,
  DROP_ROLE,
  GRANT_ROLE,
  GRANT_USER,
  GRANT_USER_ROLE,
  REVOKE_USER,
  REVOKE_ROLE,
  REVOKE_USER_ROLE,
  UPDATE_USER,
  LIST_USER,
  LIST_ROLE,
  LIST_USER_PRIVILEGE,
  LIST_ROLE_PRIVILEGE;

  /**
   * deserialize short number.
   *
   * @param i short number
   * @return NamespaceType
   */
  public static AuthorType deserialize(short i) {
    switch (i) {
      case 0:
        return CREATE_USER;
      case 1:
        return CREATE_ROLE;
      case 2:
        return DROP_USER;
      case 3:
        return DROP_ROLE;
      case 4:
        return GRANT_ROLE;
      case 5:
        return GRANT_USER;
      case 6:
        return GRANT_USER_ROLE;
      case 7:
        return REVOKE_USER;
      case 8:
        return REVOKE_ROLE;
      case 9:
        return REVOKE_USER_ROLE;
      case 10:
        return UPDATE_USER;
      case 11:
        return LIST_USER;
      case 12:
        return LIST_ROLE;
      case 13:
        return LIST_USER_PRIVILEGE;
      case 14:
        return LIST_ROLE_PRIVILEGE;
      default:
        return null;
    }
  }

  /**
   * serialize.
   *
   * @return short number
   */
  public short serialize() {
    switch (this) {
      case CREATE_USER:
        return 0;
      case CREATE_ROLE:
        return 1;
      case DROP_USER:
        return 2;
      case DROP_ROLE:
        return 3;
      case GRANT_ROLE:
        return 4;
      case GRANT_USER:
        return 5;
      case GRANT_USER_ROLE:
        return 6;
      case REVOKE_USER:
        return 7;
      case REVOKE_ROLE:
        return 8;
      case REVOKE_USER_ROLE:
        return 9;
      case UPDATE_USER:
        return 10;
      case LIST_USER:
        return 11;
      case LIST_ROLE:
        return 12;
      case LIST_USER_PRIVILEGE:
        return 13;
      case LIST_ROLE_PRIVILEGE:
        return 14;
      default:
        return -1;
    }
  }
}
