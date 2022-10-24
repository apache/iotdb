package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogUtils {
  private static final Logger logger = LoggerFactory.getLogger(AuditLogUtils.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

  public static final String LOG = "log";
  public static final String TYPE = "type";
  public static final String AUDIT_LOG_DEVICE = "root.system.audit.'%s'";
  public static final String TYPE_QUERY = "QUERY";
  public static final String TYPE_LOGIN = "LOGIN";
  public static final String TYPE_LOGOUT = "LOGOUT";
  public static final String TYPE_INSERT = "INSERT";
  public static final String TYPE_DELETE = "DELETE";
  public static final String LOG_LEVEL_IOTDB = "IOTDB";
  public static final String LOG_LEVEL_LOGGER = "LOGGER";
  public static final String LOG_LEVEL_NONE = "NONE";

  public static void writeAuditLog(String type, String log) {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    String auditLogStorage = config.getAuditLogStorage();
    SessionManager instance = SessionManager.getInstance();
    String username = instance.getUsername(instance.getCurrSessionId());
    if (LOG_LEVEL_IOTDB.equals(auditLogStorage)) {
      try {
        InsertRowPlan insertRowPlan =
            new InsertRowPlan(
                new PartialPath(String.format(AUDIT_LOG_DEVICE, username)),
                DateTimeUtils.currentTime(),
                new String[] {TYPE, LOG},
                new String[] {type, log});
        IoTDB.serviceProvider.getExecutor().insert(insertRowPlan);
      } catch (IllegalPathException | QueryProcessException e) {
        logger.error("write audit log series error,", e);
      }
    } else if (LOG_LEVEL_LOGGER.equals(auditLogStorage)) {
      if (type.contains(TYPE_INSERT)) {
        AUDIT_LOGGER.debug("user:{},type:{},action:{}", username, type, log);
      } else {
        AUDIT_LOGGER.info("user:{},type:{},action:{}", username, type, log);
      }
    }
  }
}
