package org.apache.iotdb.commons.exception;

import org.apache.iotdb.commons.conf.CommonDescriptor;

public class TTLException extends Exception {

  public TTLException(String path) {
    super(
        String.format(
            "Illegal pattern path: %s, pattern path should end with **, otherwise, it should be a specific database or device path without *",
            path));
  }

  public TTLException() {
    super(
        String.format(
            "The number of TTL stored in the system has reached threshold %d, please increase the ttl_count parameter.",
            CommonDescriptor.getInstance().getConfig().getTTLCount()));
  }
}
