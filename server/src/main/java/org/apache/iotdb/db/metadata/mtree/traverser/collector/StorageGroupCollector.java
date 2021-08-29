package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public abstract class StorageGroupCollector<T> extends CollectorTraverser<T>{

    protected boolean collectInternal = false;

    public StorageGroupCollector(IMNode startNode, String[] nodes, T resultSet) {
        super(startNode, nodes, resultSet);
    }

    public StorageGroupCollector(IMNode startNode, String[] nodes, T resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
    }

    @Override
    protected boolean isValid(IMNode node) {
        return node.isStorageGroup();
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        if(collectInternal){
            processValidNode(node, idx);
        }
        return true;
    }

    public void setCollectInternal(boolean collectInternal){
        this.collectInternal = collectInternal;
    }

}
