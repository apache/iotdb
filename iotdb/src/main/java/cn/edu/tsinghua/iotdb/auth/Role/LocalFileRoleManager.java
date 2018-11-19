package cn.edu.tsinghua.iotdb.auth.Role;

public class LocalFileRoleManager extends BasicRoleManager {

    public LocalFileRoleManager(String roleDirPath) {
        super(new LocalFileRoleAccessor(roleDirPath));
    }
}
