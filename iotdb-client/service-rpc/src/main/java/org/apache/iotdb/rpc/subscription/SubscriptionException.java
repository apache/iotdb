package org.apache.iotdb.rpc.subscription;

import java.util.Objects;

public class SubscriptionException extends RuntimeException {

  public SubscriptionException(String message) {
    super(message);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof SubscriptionException
        && Objects.equals(getMessage(), ((SubscriptionException) obj).getMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage());
  }
}
