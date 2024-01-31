package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MNodeUpdateChildBuffer extends MNodeChildBuffer{
    @Nullable
    @Override
    public synchronized ICachedMNode put(String key, ICachedMNode value) {
        if(receivingBuffer == null){
            receivingBuffer = new ConcurrentHashMap<>();
        }
        if(flushingBuffer == null || !flushingBuffer.containsKey(key)){
            totalSize++;
        }
        return receivingBuffer.put(key, value);
    }

    @Nullable
    @Override
    public synchronized ICachedMNode putIfAbsent(String key, ICachedMNode value) {
        ICachedMNode result = get(receivingBuffer, key);
        if(result == null){
            if(receivingBuffer == null){
                receivingBuffer = new ConcurrentHashMap<>();
            }
            if(!flushingBuffer.containsKey(key)){
                totalSize++;
            }
            result = receivingBuffer.put(key, value);
        }
        return result;
    }

    @Override
    public synchronized void putAll(@Nonnull Map<? extends String, ? extends ICachedMNode> m) {
        if(receivingBuffer == null){
            receivingBuffer = new ConcurrentHashMap<>();
        }
        for(Entry<? extends String, ? extends ICachedMNode> entry : m.entrySet()){
            if(!flushingBuffer.containsKey(entry.getKey())){
                totalSize++;
            }
            receivingBuffer.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized ICachedMNode removeFromFlushingBuffer(Object key){
        if(flushingBuffer == null){
            return null;
        }
        ICachedMNode result = flushingBuffer.remove(key);
        if(result != null && !receivingBuffer.containsKey(key)){
            totalSize--;
        }
        return result;
    }
}
