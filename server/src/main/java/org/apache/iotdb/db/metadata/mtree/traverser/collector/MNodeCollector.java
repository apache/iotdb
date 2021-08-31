package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public abstract class MNodeCollector<T> extends CollectorTraverser<T>{

    public MNodeCollector(IMNode startNode, String[] nodes, T resultSet) {
        super(startNode, nodes, resultSet);
        isNodeTraverser = true;
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
