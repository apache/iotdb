package cn.edu.tsinghua.iotdb.auth.entity;

import cn.edu.tsinghua.iotdb.utils.AuthUtils;

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

    public Set<Integer> getPrivileges(String path) {
        return AuthUtils.getPrivileges(path, privilegeList);
    }

    public boolean checkPrivilege(String path, int privilegeId) {
        return AuthUtils.checkPrivilege(path, privilegeId, privilegeList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Role role = (Role) o;
        return Objects.equals(name, role.name) &&
                Objects.equals(privilegeList, role.privilegeList);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, privilegeList);
    }
}
