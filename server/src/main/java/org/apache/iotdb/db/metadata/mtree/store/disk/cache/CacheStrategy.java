package org.apache.iotdb.db.metadata.mtree.store.disk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.List;

public class CacheStrategy implements ICacheStrategy{
    @Override
    public void updateCacheStatusAfterRead(IMNode node) {

    }

    @Override
    public void updateCacheStatusAfterAppend(IMNode node) {

    }

    @Override
    public void updateCacheStatusAfterUpdate(IMNode node) {

    }

    @Override
    public void remove(IMNode node) {

    }

    @Override
    public List<IMNode> evict() {
        return null;
    }

    @Override
    public void pinMNode(IMNode node) {

    }

    @Override
    public void unPinMNode(IMNode node) {

    }

    @Override
    public void clear() {

    }
}
