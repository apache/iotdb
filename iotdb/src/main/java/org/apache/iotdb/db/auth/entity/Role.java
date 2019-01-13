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
package org.apache.iotdb.db.auth.entity;

import org.apache.iotdb.db.utils.AuthUtils;
import org.apache.iotdb.db.utils.AuthUtils;

import java.util.*;

/**
 * This class contains all information of a role.
 */
public class Role {
    public String name;
    public List<PathPrivilege> privilegeList;

    public Role() {
    }

    public Role(String name) {
        this.name = name;
        this.privilegeList = new ArrayList<>();
    }

    public boolean hasPrivilege(String path, int privilegeId) {
        return AuthUtils.hasPrivilege(path, privilegeId, privilegeList);
    }

    public void addPrivilege(String path, int privilgeId) {
        AuthUtils.addPrivilege(path, privilgeId, privilegeList);
    }

    public void removePrivilege(String path, int privilgeId) {
        AuthUtils.removePrivilege(path, privilgeId, privilegeList);
    }

    public void setPrivileges(String path, Set<Integer> privileges) {
        for (PathPrivilege pathPrivilege : privilegeList) {
            if (pathPrivilege.path.equals(path))
                pathPrivilege.privileges = privileges;
        }
    }

    public Set<Integer> getPrivileges(String path) {
        return AuthUtils.getPrivileges(path, privilegeList);
    }

    public boolean checkPrivilege(String path, int privilegeId) {
        return AuthUtils.checkPrivilege(path, privilegeId, privilegeList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Role role = (Role) o;
        return Objects.equals(name, role.name) && Objects.equals(privilegeList, role.privilegeList);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, privilegeList);
    }
}
