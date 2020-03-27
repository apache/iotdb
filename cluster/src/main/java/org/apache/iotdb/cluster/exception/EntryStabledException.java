package org.apache.iotdb.cluster.exception;

public class EntryStabledException extends Exception {

  public EntryStabledException(){
    super("requested index is unavailable in UnCommitEntryManager due to persistence.");
  }
}
