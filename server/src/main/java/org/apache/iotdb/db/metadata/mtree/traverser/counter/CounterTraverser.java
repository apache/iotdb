package org.apache.iotdb.db.metadata.mtree.traverser.counter;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;

public abstract class CounterTraverser extends Traverser {

    int count;

    @Override
    protected void processValidNode(IMNode node, int idx) throws MetadataException {
        count ++;
    }

    public int getCount(){
        return count;
    }
}
