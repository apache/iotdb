package org.apache.iotdb.db.metadata.mtree.traverser.counter;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public class MNodeLevelCounter extends CounterTraverser{

    public MNodeLevelCounter(IMNode startNode, String[] nodes, int targetLevel) {
        super(startNode, nodes);
        isLevelTraverser = true;
        this.targetLevel = targetLevel;
    }

    @Override
    protected boolean isValid(IMNode node) {
        return true;
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        return false;
    }
}
