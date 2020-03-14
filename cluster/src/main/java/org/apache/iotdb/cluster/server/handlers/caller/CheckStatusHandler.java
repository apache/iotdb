package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.thrift.async.AsyncMethodCallback;

public class CheckStatusHandler implements AsyncMethodCallback<CheckStatusResponse> {

  CheckStatusResponse checkStatusResponse;

  public CheckStatusHandler() {
    this.checkStatusResponse = new CheckStatusResponse();
  }

  public CheckStatusResponse getCheckStatusResponse() {
    return checkStatusResponse;
  }

  @Override
  public void onComplete(CheckStatusResponse response) {
    this.checkStatusResponse = response;
  }

  @Override
  public void onError(Exception e) {

  }
}
