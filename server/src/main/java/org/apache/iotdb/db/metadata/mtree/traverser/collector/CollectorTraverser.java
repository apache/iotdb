package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;

public abstract class CollectorTraverser extends Traverser {

    protected boolean needLast = false;
    protected int limit;
    protected int offset;

    protected boolean hasLimit = false;
    protected int count = 0;
    protected int curOffset = -1;

    public void traverse(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {
        if (hasLimit && count == limit) {
            return;
        }
        super.traverse(node, idx, multiLevelWildcard);
    }

    public int getOffset() {
        return offset;
    }

    public void setNeedLast(boolean needLast) {
        this.needLast = needLast;
    }

    public void setLimit(int limit) {
        this.limit = limit;
        if (limit != 0) {
            hasLimit = true;
        }
    }

    public void setOffset(int offset) {
        this.offset = offset;
        if (offset != 0) {
            hasLimit = true;
        }
    }
}
