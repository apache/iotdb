/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.physical.sys;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;

public class AuthorPlan extends PhysicalPlan {
    private final AuthorOperator.AuthorType authorType;
    private String userName;
    private String roleName;
    private String password;
    private String newPassword;
    private Set<Integer> permissions;
    private Path nodeName;

    public AuthorPlan(AuthorOperator.AuthorType authorType, String userName, String roleName, String password,
            String newPassword, String[] authorizationList, Path nodeName) throws AuthException {
        super(false, Operator.OperatorType.AUTHOR);
        this.authorType = authorType;
        this.userName = userName;
        this.roleName = roleName;
        this.password = password;
        this.newPassword = newPassword;
        this.permissions = strToPermissions(authorizationList);
        this.nodeName = nodeName;
        switch (authorType) {
        case DROP_ROLE:
            this.setOperatorType(Operator.OperatorType.DELETE_ROLE);
            break;
        case DROP_USER:
            this.setOperatorType(Operator.OperatorType.DELETE_USER);
            break;
        case GRANT_ROLE:
            this.setOperatorType(Operator.OperatorType.GRANT_ROLE_PRIVILEGE);
            break;
        case GRANT_USER:
            this.setOperatorType(Operator.OperatorType.GRANT_USER_PRIVILEGE);
            break;
        case CREATE_ROLE:
            this.setOperatorType(Operator.OperatorType.CREATE_ROLE);
            break;
        case CREATE_USER:
            this.setOperatorType(Operator.OperatorType.CREATE_USER);
            break;
        case REVOKE_ROLE:
            this.setOperatorType(Operator.OperatorType.REVOKE_ROLE_PRIVILEGE);
            break;
        case REVOKE_USER:
            this.setOperatorType(Operator.OperatorType.REVOKE_USER_PRIVILEGE);
            break;
        case UPDATE_USER:
            this.setOperatorType(Operator.OperatorType.MODIFY_PASSWORD);
            break;
        case GRANT_ROLE_TO_USER:
            this.setOperatorType(Operator.OperatorType.GRANT_ROLE_PRIVILEGE);
            break;
        case REVOKE_ROLE_FROM_USER:
            this.setOperatorType(Operator.OperatorType.REVOKE_USER_ROLE);
            break;
        case LIST_USER_PRIVILEGE:
            this.setOperatorType(Operator.OperatorType.LIST_USER_PRIVILEGE);
            break;
        case LIST_ROLE_PRIVILEGE:
            this.setOperatorType(Operator.OperatorType.LIST_ROLE_PRIVILEGE);
            break;
        case LIST_USER_ROLES:
            this.setOperatorType(Operator.OperatorType.LIST_USER_ROLES);
            break;
        case LIST_ROLE_USERS:
            this.setOperatorType(Operator.OperatorType.LIST_ROLE_USERS);
            break;
        case LIST_USER:
            this.setOperatorType(Operator.OperatorType.LIST_USER);
            break;
        case LIST_ROLE:
            this.setOperatorType(Operator.OperatorType.LIST_ROLE);
            break;
        }
    }

    public AuthorOperator.AuthorType getAuthorType() {
        return authorType;
    }

    public String getUserName() {
        return userName;
    }

    public String getRoleName() {
        return roleName;
    }

    public String getPassword() {
        return password;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public Set<Integer> getPermissions() {
        return permissions;
    }

    public Path getNodeName() {
        return nodeName;
    }

    private Set<Integer> strToPermissions(String[] authorizationList) throws AuthException {
        Set<Integer> result = new HashSet<>();
        if (authorizationList == null) {
            return result;
        }
        for (String s : authorizationList) {
            PrivilegeType[] types = PrivilegeType.values();
            boolean legal = false;
            for (PrivilegeType privilegeType : types) {
                if (s.equalsIgnoreCase(privilegeType.name())) {
                    result.add(privilegeType.ordinal());
                    legal = true;
                    break;
                }
            }
            if (!legal) {
                throw new AuthException("No such privilege " + s);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "userName: " + userName + "\nroleName: " + roleName + "\npassword: " + password + "\nnewPassword: "
                + newPassword + "\npermissions: " + permissions + "\nnodeName: " + nodeName + "\nauthorType: "
                + authorType;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (nodeName != null) {
            ret.add(nodeName);
        }
        return ret;
    }
}
