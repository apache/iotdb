package org.apache.iotdb.db.service.basic.dto;

import org.apache.iotdb.rpc.TSStatusCode;

public class OpenSessionResp {
  private long sessionId;
  private String loginMessage;
  private TSStatusCode tsStatusCode;

  public long getSessionId() {
    return sessionId;
  }

  public void setSessionId(long sessionId) {
    this.sessionId = sessionId;
  }

  public String getLoginMessage() {
    return loginMessage;
  }

  public void setLoginMessage(String loginMessage) {
    this.loginMessage = loginMessage;
  }

  public TSStatusCode getTsStatusCode() {
    return tsStatusCode;
  }

  public void setTsStatusCode(TSStatusCode tsStatusCode) {
    this.tsStatusCode = tsStatusCode;
  }
}
