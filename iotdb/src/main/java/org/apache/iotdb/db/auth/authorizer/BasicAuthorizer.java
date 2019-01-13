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
package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.Role.IRoleManager;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.auth.user.IUserManager;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.AuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

abstract public class BasicAuthorizer implements IAuthorizer, IService {

    private static final Logger logger = LoggerFactory.getLogger(BasicAuthorizer.class);
    private static final Set<Integer> ADMIN_PRIVILEGES;

    static {
        ADMIN_PRIVILEGES = new HashSet<>();
        for (int i = 0; i < PrivilegeType.values().length; i++)
            ADMIN_PRIVILEGES.add(i);
    }

    private IUserManager userManager;
    private IRoleManager roleManager;

    BasicAuthorizer(IUserManager userManager, IRoleManager roleManager) throws AuthException {
        this.userManager = userManager;
        this.roleManager = roleManager;
        init();
    }

    protected void init() throws AuthException {
        userManager.reset();
        roleManager.reset();
        logger.info("Initialization of Authorizer completes");
    }

    @Override
    public boolean login(String username, String password) throws AuthException {
        User user = userManager.getUser(username);
        return user != null && user.password.equals(AuthUtils.encryptPassword(password));
    }

    @Override
    public boolean createUser(String username, String password) throws AuthException {
        return userManager.createUser(username, password);
    }

    @Override
    public boolean deleteUser(String username) throws AuthException {
        if (IoTDBConstant.ADMIN_NAME.equals(username))
            throw new AuthException("Default administrator cannot be deleted");
        return userManager.deleteUser(username);
    }

    @Override
    public boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException {
        if (IoTDBConstant.ADMIN_NAME.equals(username))
            throw new AuthException("Invalid operation, administrator already has all privileges");
        if (!PrivilegeType.isPathRelevant(privilegeId))
            path = IoTDBConstant.PATH_ROOT;
        return userManager.grantPrivilegeToUser(username, path, privilegeId);
    }

    @Override
    public boolean revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException {
        if (IoTDBConstant.ADMIN_NAME.equals(username))
            throw new AuthException("Invalid operation, administrator must have all privileges");
        if (!PrivilegeType.isPathRelevant(privilegeId))
            path = IoTDBConstant.PATH_ROOT;
        return userManager.revokePrivilegeFromUser(username, path, privilegeId);
    }

    @Override
    public boolean createRole(String roleName) throws AuthException {
        return roleManager.createRole(roleName);
    }

    @Override
    public boolean deleteRole(String roleName) throws AuthException {
        boolean success = roleManager.deleteRole(roleName);
        if (!success)
            return false;
        else {
            // proceed to revoke the role in all users
            List<String> users = userManager.listAllUsers();
            for (String user : users) {
                try {
                    userManager.revokeRoleFromUser(roleName, user);
                } catch (AuthException e) {
                    logger.warn("Error encountered when revoking a role {} from user {} after deletion, because {}",
                            roleName, user, e);
                }
            }
        }
        return true;
    }

    @Override
    public boolean grantPrivilegeToRole(String roleName, String path, int privilegeId) throws AuthException {
        if (!PrivilegeType.isPathRelevant(privilegeId))
            path = IoTDBConstant.PATH_ROOT;
        return roleManager.grantPrivilegeToRole(roleName, path, privilegeId);
    }

    @Override
    public boolean revokePrivilegeFromRole(String roleName, String path, int privilegeId) throws AuthException {
        if (!PrivilegeType.isPathRelevant(privilegeId))
            path = IoTDBConstant.PATH_ROOT;
        return roleManager.revokePrivilegeFromRole(roleName, path, privilegeId);
    }

    @Override
    public boolean grantRoleToUser(String roleName, String username) throws AuthException {
        Role role = roleManager.getRole(roleName);
        if (role == null) {
            throw new AuthException(String.format("No such role : %s", roleName));
        }
        // the role may be deleted before it ts granted to the user, so a double check is necessary.
        boolean success = userManager.grantRoleToUser(roleName, username);
        if (success) {
            role = roleManager.getRole(roleName);
            if (role == null) {
                throw new AuthException(String.format("No such role : %s", roleName));
            } else
                return true;
        } else
            return false;
    }

    @Override
    public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        Role role = roleManager.getRole(roleName);
        if (role == null) {
            throw new AuthException(String.format("No such role : %s", roleName));
        }
        return userManager.revokeRoleFromUser(roleName, username);
    }

    @Override
    public Set<Integer> getPrivileges(String username, String path) throws AuthException {
        if (IoTDBConstant.ADMIN_NAME.equals(username))
            return ADMIN_PRIVILEGES;
        User user = userManager.getUser(username);
        if (user == null) {
            throw new AuthException(String.format("No such user : %s", username));
        }
        // get privileges of the user
        Set<Integer> privileges = user.getPrivileges(path);
        // merge the privileges of the roles of the user
        for (String roleName : user.roleList) {
            Role role = roleManager.getRole(roleName);
            if (role != null) {
                privileges.addAll(role.getPrivileges(path));
            }
        }
        return privileges;
    }

    @Override
    public boolean updateUserPassword(String username, String newPassword) throws AuthException {
        return userManager.updateUserPassword(username, newPassword);
    }

    @Override
    public boolean checkUserPrivileges(String username, String path, int privilegeId) throws AuthException {
        if (IoTDBConstant.ADMIN_NAME.equals(username))
            return true;
        User user = userManager.getUser(username);
        if (user == null) {
            throw new AuthException(String.format("No such user : %s", username));
        }
        // get privileges of the user
        if (user.checkPrivilege(path, privilegeId))
            return true;
        // merge the privileges of the roles of the user
        for (String roleName : user.roleList) {
            Role role = roleManager.getRole(roleName);
            if (role.checkPrivilege(path, privilegeId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void reset() throws AuthException {
        init();
    }

    @Override
    public void start() throws StartupException {
        try {
            init();
        } catch (AuthException e) {
            throw new StartupException(e.getMessage());
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public ServiceType getID() {
        return ServiceType.AUTHORIZATION_SERVICE;
    }

    @Override
    public List<String> listAllUsers() {
        return userManager.listAllUsers();
    }

    @Override
    public List<String> listAllRoles() {
        return roleManager.listAllRoles();
    }

    @Override
    public Role getRole(String roleName) throws AuthException {
        return roleManager.getRole(roleName);
    }

    @Override
    public User getUser(String username) throws AuthException {
        return userManager.getUser(username);
    }
}
