package org.apache.iotdb.commons.audit;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;

public interface IAuditEntity {

  long getUserId();

  String getUsername();

  String getCliHostname();

  AuditEventType getAuditEventType();

  IAuditEntity setAuditEventType(AuditEventType auditEventType);

  AuditLogOperation getAuditLogOperation();

  IAuditEntity setAuditLogOperation(AuditLogOperation auditLogOperation);

  PrivilegeType getPrivilegeType();

  IAuditEntity setPrivilegeType(PrivilegeType privilegeType);

  boolean getResult();

  IAuditEntity setResult(boolean result);

  String getDatabase();

  String getSqlString();
}
