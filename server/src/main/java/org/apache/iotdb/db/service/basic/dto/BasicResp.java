package org.apache.iotdb.db.service.basic.dto;

import org.apache.iotdb.rpc.TSStatusCode;

public class BasicResp {

  private String message;
  private TSStatusCode tsStatusCode;

  public BasicResp() {}

  public BasicResp(TSStatusCode tsStatusCode, String message) {
    this.message = message;
    this.tsStatusCode = tsStatusCode;
  }

  public BasicResp(String message) {
    this.message = message;
  }

  public BasicResp(TSStatusCode tsStatusCode) {
    this.tsStatusCode = tsStatusCode;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public TSStatusCode getTsStatusCode() {
    return tsStatusCode;
  }

  public void setTsStatusCode(TSStatusCode tsStatusCode) {
    this.tsStatusCode = tsStatusCode;
  }
}
