package org.apache.iotdb.db.metadata.mtree.traverser.counter;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public class EntityCounter extends CounterTraverser{

    public EntityCounter(IMNode startNode, String[] nodes) {
        super(startNode, nodes);
    }

    @Override
    protected boolean isValid(IMNode node) {
        return node.isEntity();
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        return false;
    }
}
