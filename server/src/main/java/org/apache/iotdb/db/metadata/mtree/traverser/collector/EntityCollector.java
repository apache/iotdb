package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public abstract class EntityCollector<T> extends CollectorTraverser<T>{

    public EntityCollector(IMNode startNode, String[] nodes, T resultSet) {
        super(startNode, nodes, resultSet);
    }

    public EntityCollector(IMNode startNode, String[] nodes, T resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
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
