package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.rpc.thrift.Node;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

public class ShowNowHandler implements AsyncMethodCallback<ByteBuffer> {
  private static final Logger logger = LoggerFactory.getLogger(ShowNowHandler.class);

  private Node contact;
  private AtomicReference<ByteBuffer> result;

  @Override
  public void onComplete(ByteBuffer response) {
    logger.info("Received show now from {}", contact);
    synchronized (result) {
      result.set(response);
      result.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.warn("Cannot get show now from {}, because ", contact, exception);
  }

  public void setResponse(AtomicReference<ByteBuffer> response) {
    this.result = response;
  }

  public void setContact(Node contact) {
    this.contact = contact;
  }
}
