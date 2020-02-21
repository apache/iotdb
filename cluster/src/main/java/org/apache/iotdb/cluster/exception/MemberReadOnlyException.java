package org.apache.iotdb.cluster.exception;

import org.apache.iotdb.cluster.rpc.thrift.Node;

public class MemberReadOnlyException extends Exception{

  public MemberReadOnlyException(Node node) {
    super(String.format("The node %s has been set readonly for the partitions, please retry to find "
        + "a new node", node));
  }
}
