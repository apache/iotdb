package org.apache.iotdb.cluster.server.handlers.caller;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNodesListHandler implements AsyncMethodCallback<List<String>> {

  private static final Logger logger = LoggerFactory.getLogger(GetNodesListHandler.class);

  private Node contact;
  private AtomicReference<List<String>> result;

  @Override
  public void onComplete(List<String> resp) {
    logger.info("Received node lists of size {} from {}", resp.size(), contact);
    synchronized (result) {
      result.set(resp);
      result.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.warn("Cannot get node list from {}, because", contact, exception);
  }

  public void setResponse(AtomicReference response) {
    this.result = response;
  }

  public void setContact(Node contact) {
    this.contact = contact;
  }
}