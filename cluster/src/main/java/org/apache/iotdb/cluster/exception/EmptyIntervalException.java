package org.apache.iotdb.cluster.exception;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class EmptyIntervalException extends Exception {

  public EmptyIntervalException(Filter filter) {
    super(String.format("The interval of the filter %s is empty.", filter));
  }
}
