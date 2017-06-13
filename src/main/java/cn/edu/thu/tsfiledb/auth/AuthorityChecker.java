package cn.edu.thu.tsfiledb.auth;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.auth.model.Permission;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;

public class AuthorityChecker {

    private static final String SUPER_USER = "root";
    private static final Logger logger = LoggerFactory.getLogger(AuthorityChecker.class);

    public static boolean check(String username, List<Path> paths, OperatorType type) {
        if (SUPER_USER.equals(username)) {
            return true;
        }
        int permission = translateToPermissionId(type);
        if (permission == -1) {
            logger.error("OperateType not found. {}", type);
            return false;
        }
        for (int i = 0; i < paths.size(); i++) {
            if (!checkOnePath(username, paths.get(i), permission)) {
                return false;
            }
        }
        return true;
    }

    private static List<String> getAllParentPath(Path path) {
        List<String> parentPaths = new ArrayList<String>();
        String fullPath = path.getFullPath();
        String[] nodes = fullPath.split("\\.");

        for (int i = 0; i < nodes.length; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                sb.append(nodes[j]);
                if (j < i) {
                    sb.append(".");
                }
            }
            parentPaths.add(sb.toString());
        }
        return parentPaths;
    }

    private static boolean checkOnePath(String username, Path path, int permission) {
        List<String> parentPaths = getAllParentPath(path);
        for (int i = 0; i < parentPaths.size(); i++) {
            if (Authorizer.checkUserPermission(username, parentPaths.get(i), permission)) {
                return true;
            }
        }
        return false;
    }

    private static int translateToPermissionId(OperatorType type) {
        switch (type) {
            case METADATA:
                return Permission.CREATE;
            case QUERY:
            case SELECT:
            case FILTER:
            case GROUPBY:
            case SEQTABLESCAN:
            case TABLESCAN:
                return Permission.READ;
            case DELETE:
                return Permission.DELETE;
            case MULTIINSERT:
            case LOADDATA:
                return Permission.INSERT;
            case UPDATE:
                return Permission.MODIFY;
            case AUTHOR:
            case BASIC_FUNC:
            case FILEREAD:
            case FROM:
            case FUNC:
            case HASHTABLESCAN:
            case JOIN:
            case LIMIT:
            case MERGEJOIN:
            case NULL:
            case ORDERBY:
            case PROPERTY:
            case SFW:
            case UNION:

                return -1;
            default:
                return -1;
        }

    }
}
