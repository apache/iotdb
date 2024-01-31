package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container;

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.util.Iterator;
import java.util.Map;

public interface IMNodeChildBuffer extends IMNodeContainer<ICachedMNode> {

    Iterator<ICachedMNode> getMNodeChildBufferIterator();

    Map<String, ICachedMNode> getFlushingBuffer();

    Map<String, ICachedMNode> getReceivingBuffer();

    void transferReceivingBufferToFlushingBuffer();

    ICachedMNode removeFromFlushingBuffer(Object key);
}
