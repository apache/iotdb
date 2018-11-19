package cn.edu.tsinghua.iotdb.auth.Role;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.concurrent.HashLock;
import cn.edu.tsinghua.iotdb.utils.AuthUtils;

import java.io.IOException;
import java.util.*;

/**
 * This class reads roles from local files through LocalFileRoleAccessor and manages them in a hash map.
 */
public abstract class BasicRoleManager implements IRoleManager{

    private Map<String, Role> roleMap;
    private IRoleAccessor accessor;
    private HashLock lock;

    BasicRoleManager(LocalFileRoleAccessor accessor) {
        this.roleMap = new HashMap<>();
        this.accessor = accessor;
        this.lock = new HashLock();
    }

    @Override
    public Role getRole(String rolename) throws AuthException {
        lock.readLock(rolename);
        Role role = roleMap.get(rolename);
        try {
            if(role == null) {
                role = accessor.loadRole(rolename);
                if(role != null)
                    roleMap.put(rolename, role);
            }
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.readUnlock(rolename);
        }
        return role;
    }

    @Override
    public boolean createRole(String rolename) throws AuthException {
        AuthUtils.validateRolename(rolename);

        Role role = getRole(rolename);
        if(role != null)
            return false;
        lock.writeLock(rolename);
        try {
            role = new Role(rolename);
            accessor.saveRole(role);
            roleMap.put(rolename, role);
            return true;
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public boolean deleteRole(String rolename) throws AuthException {
        lock.writeLock(rolename);
        try {
            if(accessor.deleteRole(rolename)) {
                roleMap.remove(rolename);
                return true;
            } else
                return false;
        } catch (IOException e) {
            throw new AuthException(e);
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public boolean grantPrivilegeToRole(String rolename, String path, int privilegeId) throws AuthException {
        AuthUtils.validatePrivilegeOnPath(path, privilegeId);
        lock.writeLock(rolename);
        try {
            Role role = getRole(rolename);
            if(role == null) {
                throw new AuthException(String.format("No such role %s", rolename));
            }
            if(role.hasPrivilege(path, privilegeId)) {
                return false;
            }
            Set<Integer> privilegesCopy = new HashSet<>(role.getPrivileges(path));
            role.addPrivilege(path, privilegeId);
            try {
                accessor.saveRole(role);
            } catch (IOException e) {
                role.setPrivileges(path, privilegesCopy);
                throw new AuthException(e);
            }
            return true;
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public boolean revokePrivilegeFromRole(String rolename, String path, int privilegeId) throws AuthException {
        AuthUtils.validatePrivilegeOnPath(path, privilegeId);
        lock.writeLock(rolename);
        try {
            Role role = getRole(rolename);
            if(role == null) {
                throw new AuthException(String.format("No such role %s", rolename));
            }
            if(!role.hasPrivilege(path, privilegeId)) {
                return false;
            }
            role.removePrivilege(path, privilegeId);
            try {
                accessor.saveRole(role);
            } catch (IOException e) {
                role.addPrivilege(path, privilegeId);
                throw new AuthException(e);
            }
            return true;
        } finally {
            lock.writeUnlock(rolename);
        }
    }

    @Override
    public void reset() {
        accessor.reset();
        roleMap.clear();
        lock.reset();
    }

    @Override
    public List<String> listAllRoles() {
        List<String> rtlist = accessor.listAllRoles();
        rtlist.sort(null);
        return rtlist;
    }
}
