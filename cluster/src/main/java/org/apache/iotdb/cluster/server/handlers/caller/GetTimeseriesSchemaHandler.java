package org.apache.iotdb.cluster.server.handlers.caller;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetTimeseriesSchemaHandler implements AsyncMethodCallback<List<List<String>>> {

  private static final Logger logger = LoggerFactory.getLogger(GetTimeseriesSchemaHandler.class);

  private Node contact;
  private AtomicReference<List<List<String>>> result;

  @Override
  public void onComplete(List<List<String>> resp) {
    logger.info("Received timeseries schema from {}", contact);
    synchronized (result) {
      result.set(resp);
      result.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.warn("Cannot get timeseries schema from {}, because ", contact, exception);
  }

  public void setResponse(AtomicReference response) {
    this.result = response;
  }

  public void setContact(Node contact) {
    this.contact = contact;
  }
}
