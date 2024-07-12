package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.rpc.TimeoutChangeableTFastFramedTransport;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.net.InetAddress;
import java.util.Optional;

/**
 * Special context based on a thread-local allowing to pass information that is used by only very
 * few services bia Java's ThreadLocals.
 */
public class RequestContext {

  public static final ThreadLocal<RequestContext> contextStorage = new ThreadLocal<>();

  public static void set(TTransport remoteTransport) {
    contextStorage.set(new RequestContext(remoteTransport));
  }

  public static RequestContext get() {
    return contextStorage.get();
  }

  public static void remove() {
    contextStorage.remove();
  }

  private final TTransport remoteTransport;

  private RequestContext(TTransport remoteTransport) {
    this.remoteTransport = remoteTransport;
  }

  public TTransport getRemoteTransport() {
    return remoteTransport;
  }

  public Optional<InetAddress> getRemoteAddress() {
    if (remoteTransport instanceof TimeoutChangeableTFastFramedTransport) {
      TimeoutChangeableTFastFramedTransport castedTransport =
          (TimeoutChangeableTFastFramedTransport) remoteTransport;
      if (castedTransport.getSocket() instanceof TSocket) {
        return Optional.of(((TSocket) castedTransport.getSocket()).getSocket().getInetAddress());
      }
    }
    return Optional.empty();
  }
}
