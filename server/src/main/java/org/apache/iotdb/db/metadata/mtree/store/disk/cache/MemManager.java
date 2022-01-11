package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

public class MemManager implements IMemManager{
    @Override
    public void initCapacity(int capacity) {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isUnderThreshold() {
        return false;
    }

    @Override
    public void requestMemResource(IMNode node, MemEvictionCallBack evictionCallBack) {

    }

    @Override
    public void releaseMemResource(IMNode node) {

    }
}
