package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

public interface IMemManager {

    void initCapacity(int capacity);

    boolean isEmpty();

    boolean isUnderThreshold();

    void requestMemResource(IMNode node,MemEvictionCallBack evictionCallBack);

    void releaseMemResource(IMNode node);

    @FunctionalInterface
    interface MemEvictionCallBack{

        boolean call();
    }

}
