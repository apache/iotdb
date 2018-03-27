package cn.edu.tsinghua.iotdb.auth.authorizer;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.Role.LocalFileRoleManager;
import cn.edu.tsinghua.iotdb.auth.user.LocalFileUserManager;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class LocalFileAuthorizer extends BasicAuthorizer {
    private static TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
    private static Logger logger = LoggerFactory.getLogger(LocalFileAuthorizer.class);

    private LocalFileAuthorizer() throws AuthException {
        super(new LocalFileUserManager(config.dataDir + File.separator + "users" + File.separator),
                new LocalFileRoleManager(config.dataDir + File.separator + "roles" + File.separator));
    }

    private static class InstanceHolder {
        private static LocalFileAuthorizer instance;

        static {
            try {
                instance = new LocalFileAuthorizer();
            } catch (AuthException e) {
                logger.error("Authorizer initialization failed due to ", e);
                instance = null;
            }
        }
    }

    public static LocalFileAuthorizer getInstance() throws AuthException {
        if(InstanceHolder.instance == null)
            throw new AuthException("Authorizer uninitialized");
        return InstanceHolder.instance;
    }
}
