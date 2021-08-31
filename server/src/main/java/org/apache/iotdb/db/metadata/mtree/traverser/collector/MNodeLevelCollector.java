package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public abstract class MNodeLevelCollector<T> extends CollectorTraverser<T>{

    public MNodeLevelCollector(IMNode startNode, String[] nodes, T resultSet, int targetLevel) {
        super(startNode, nodes, resultSet);
        isNodeTraverser = true;
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
