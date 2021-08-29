package org.apache.iotdb.db.metadata.mtree.traverser.counter;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

public class StorageGroupCounter extends CounterTraverser{
    @Override
    protected boolean isValid(IMNode node) {
        return node.isStorageGroup();
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        return true;
    }
}
