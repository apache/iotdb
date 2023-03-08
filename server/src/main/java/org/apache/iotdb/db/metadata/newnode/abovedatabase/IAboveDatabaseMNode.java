package org.apache.iotdb.db.metadata.newnode.abovedatabase;

import org.apache.iotdb.db.metadata.mnode.IMNode;

/** This interface defines a DatabaseMNode's operation interfaces. */
public interface IAboveDatabaseMNode<N extends IMNode<?>> extends IMNode<N> {
  N getAsMNode();
}
