package org.apache.iotdb.db.metadata.mnode;

/**
 * when user create a measurement directly under the storage group, create an EmptyDeviceMNode
 * between the StorageGroupMNode and LeafMNode
 *
 * e.g.,
 * set storage group to root.sg
 * create timeseries root.sg.s1
 *
 * MTree: root.StorageGroupMNode(sg).EmptyDeviceMNode.LeafMNode(s1)
 *
 * when getFullPath() at s1, skip the EmptyDeviceMNode and return root.sg.s1
 *
 */
public class EmptyDeviceMNode extends DeviceMNode {

  public static final String NAME = "";

  public EmptyDeviceMNode(MNode parent) {
    super(parent, NAME);
  }
}
