package org.apache.iotdb.db.auth.Role;

public class LocalFileRoleManager extends BasicRoleManager {

    public LocalFileRoleManager(String roleDirPath) {
        super(new LocalFileRoleAccessor(roleDirPath));
    }
}
