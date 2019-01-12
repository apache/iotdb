package org.apache.iotdb.db.auth.user;

import org.apache.iotdb.db.auth.AuthException;

public class LocalFileUserManager extends BasicUserManager {
    public LocalFileUserManager(String userDirPath) throws AuthException {
        super(new LocalFileUserAccessor(userDirPath));
    }
}
