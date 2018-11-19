package cn.edu.tsinghua.iotdb.auth.user;

import cn.edu.tsinghua.iotdb.auth.AuthException;

public class LocalFileUserManager extends BasicUserManager {
    public LocalFileUserManager(String userDirPath) throws AuthException {
        super(new LocalFileUserAccessor(userDirPath));
    }
}
