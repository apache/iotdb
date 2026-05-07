package com.timecho.iotdb.rpc;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.external.api.thrift.JudgableServerContext;

import java.io.IOException;
import java.net.Socket;

public class IPCheckListServerContext implements JudgableServerContext {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  Socket client;

  public IPCheckListServerContext(Socket socket) {
    this.client = socket;
  }

  @Override
  public boolean whenConnect() {
    if (config.isEnableWhiteList() || config.isEnableBlackList()) {
      String clientIP = client.getInetAddress().getHostAddress();
      // check black list and white list
      if (IPFilter.isDeniedConnect(clientIP)) {
        String reason =
            IPFilter.isInBlackList(clientIP)
                ? "rejected by blacklist"
                : "rejected: not in whitelist";
        DNAuditLogger.getInstance()
            .log(
                new AuditLogFields(
                    AuthorityChecker.INTERNAL_AUDIT_USER_ID,
                    AuthorityChecker.INTERNAL_AUDIT_USER,
                    clientIP,
                    AuditEventType.LOGIN_REJECT_IP,
                    AuditLogOperation.CONTROL,
                    false),
                () -> String.format("Connection %s, client IP: %s", reason, clientIP));
        try {
          client.close();
          return false;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return true;
  }

  @Override
  public boolean whenDisconnect() {
    return true;
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return JudgableServerContext.super.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return JudgableServerContext.super.isWrapperFor(iface);
  }
}
