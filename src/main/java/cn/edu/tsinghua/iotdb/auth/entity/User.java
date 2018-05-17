package cn.edu.tsinghua.iotdb.auth.entity;

import cn.edu.tsinghua.iotdb.utils.AuthUtils;

import java.util.*;

/**
 * This class contains all information of a User.
 */
public class User {
    public String name;
    public String password;
    public List<PathPrivilege> privilegeList;
    public List<String> roleList;
    /**
     * The latest time when the user is referenced. Reserved to provide session control or LRU mechanism in the future.
     */
    public long lastActiveTime;

    public User() {
    }

    public User(String name, String password) {
        this.name = name;
        this.password = password;
        this.privilegeList = new ArrayList<>();
        this.roleList = new ArrayList<>();
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

    public boolean hasRole(String roleName) {
        return roleList.contains(roleName);
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
        User user = (User) o;
        return lastActiveTime == user.lastActiveTime &&
                Objects.equals(name, user.name) &&
                Objects.equals(password, user.password) &&
                Objects.equals(privilegeList, user.privilegeList) &&
                Objects.equals(roleList, user.roleList);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, password, privilegeList, roleList, lastActiveTime);
    }
}
