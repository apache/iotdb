package org.apache.iotdb.commons.exception;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.rpc.TSStatusCode;

public class IllegalPrivilegeException extends MetadataException {
  private static final long serialVersionUID = 2693272249167539978L;

  public IllegalPrivilegeException(Integer priv) {
    super(String.format("%s is not a legal privilege", PrivilegeType.values()[priv].toString()));
    errorCode = TSStatusCode.ILLEGAL_PRIVILEGE.getStatusCode();
    this.isUserException = true;
  }

  public IllegalPrivilegeException(String reason) {
    super(String.format("%s", reason));
    errorCode = TSStatusCode.ILLEGAL_PATH.getStatusCode();
    this.isUserException = true;
  }
}
