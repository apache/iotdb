package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.container;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.*;

public abstract class MNodeChildBuffer implements IMNodeChildBuffer {

    protected Map<String,ICachedMNode> flushingBuffer;// 刷盘buffer，存储旧的数据节点
    protected Map<String,ICachedMNode> receivingBuffer;// 接受新到来的buffer，存储新创建或者修改后的数据节点

    protected int totalSize = 0;// 总大小 包括flushingBuffer和receivingBuffer，不包括这两个buffer的交集

    @Override
    public Iterator<ICachedMNode> getMNodeChildBufferIterator(){
        return new MNodeChildBufferIterator();
    }
    @Override
    public Map<String, ICachedMNode> getFlushingBuffer(){
        return flushingBuffer == null ? Collections.emptyMap() : flushingBuffer;
    }
    @Override
    public Map<String, ICachedMNode> getReceivingBuffer(){
        return receivingBuffer == null ? Collections.emptyMap() : receivingBuffer;
    }

    @Override
    public void transferReceivingBufferToFlushingBuffer(){
        if(flushingBuffer == null){
            flushingBuffer = new HashMap<>();
        }
        if(receivingBuffer != null){
            flushingBuffer.putAll(receivingBuffer);
            receivingBuffer.clear();
        }
    }

    @Override
    public int size() {
        return totalSize;
    }

    @Override
    public boolean isEmpty() {
        return totalSize == 0;
    }

    private boolean containKey(Map<String, ICachedMNode> map, Object key){
        return map != null && map.containsKey(key);
    }
    @Override
    public boolean containsKey(Object key) {
        return containKey(flushingBuffer, key) || containKey(receivingBuffer, key);
    }

    private boolean containValue(Map<String, ICachedMNode> map, Object value){
        return map != null && map.containsValue(value);
    }
    @Override
    public boolean containsValue(Object value) {
        return containValue(flushingBuffer, value) || containValue(receivingBuffer, value);
    }

    protected ICachedMNode get(Map<String, ICachedMNode> map, Object key){
        return map != null ? map.get(key) : null;
    }

    @Override
    public synchronized ICachedMNode get(Object key) {
        ICachedMNode result = get(receivingBuffer, key);
        if(result != null) return result;
        else {
            return get(flushingBuffer, key);
        }
    }
    private ICachedMNode remove(Map<String, ICachedMNode> map, Object key){
        return map == null ? null : map.remove(key);
    }
    @Override
    public synchronized ICachedMNode remove(Object key) {
        // 这里recevingBuffer和flushingBuffer当中存在部分重复key，所以删除的时候需要考虑
        ICachedMNode result1 = remove(flushingBuffer, key);
        ICachedMNode result2 = remove(receivingBuffer, key);
        ICachedMNode result = result1 != null ? result1 : result2;// 如果第一个为空，那么就看第二个结果；如果第一个不为空，那么第二个要么为空，要么和第一个一样
        if(result != null){
            totalSize--;
        }
        return result;
    }

    @Override
    public void clear() {
        if(receivingBuffer != null){
            receivingBuffer.clear();
        }
        if(flushingBuffer != null){
            flushingBuffer.clear();
        }
        totalSize = 0;
    }

    private Set<String> keySet(Map<String, ICachedMNode> map){
        return map == null ? Collections.emptySet() : map.keySet();
    }

    @Nonnull
    @Override
    public Set<String> keySet() {
        Set<String> result = new TreeSet<>();
        // 这个是集合结构，如果有重复，set会自动去重
        result.addAll(keySet(receivingBuffer));
        result.addAll(keySet(flushingBuffer));
        return result;
    }

    private Collection<ICachedMNode> values(Map<String, ICachedMNode> map){
        return map == null ? Collections.emptyList() : map.values();
    }

    @Nonnull
    @Override
    public Collection<ICachedMNode> values() {
        Collection<ICachedMNode> result = new HashSet<>();
        result.addAll(values(flushingBuffer));
        result.addAll(values(receivingBuffer));
        return result;
    }

    private Set<Entry<String, ICachedMNode>> entrySet(Map<String, ICachedMNode> map){
        return map == null ? Collections.emptySet() : map.entrySet();
    }

    @Nonnull
    @Override
    public Set<Entry<String, ICachedMNode>> entrySet() {
        Set<Entry<String, ICachedMNode>> result = new HashSet<>();
        // HashSet会自动去重
        result.addAll(entrySet(receivingBuffer));
        result.addAll(entrySet(flushingBuffer));
        return result;
    }

    @Nullable
    @Override
    public synchronized ICachedMNode replace(String key, ICachedMNode value) {
        throw new UnsupportedOperationException();
    }

    private class MNodeChildBufferIterator implements Iterator<ICachedMNode>{

        int flushingIndex = 0;
        int receivingIndex = 0;
        List<ICachedMNode> flushingBufferList;
        List<ICachedMNode> receivingBufferList;

        MNodeChildBufferIterator(){
            // 这里需要新建一个临时的List，因为两个Buffer当中存在重复，这里希望采用归并排序，将其合并去重
            List<ICachedMNode> list = new ArrayList<>();
            // 下面需要进行归并排序，将两个buffer当中的数据合并去重
            // 先分别获取两个buffer的values
            receivingBufferList = new ArrayList<>(receivingBuffer.values());
            flushingBufferList = new ArrayList<>(flushingBuffer.values());
            // 两个分别进行排序
            receivingBufferList.sort(Comparator.comparing(ICachedMNode::getName));
            flushingBufferList.sort(Comparator.comparing(ICachedMNode::getName));
        }

        private ICachedMNode tryGetNext(){
            // 进入这里只有三种情况，分别为，两个都有剩，和两个各自有剩，然后进行逐步归并去重
            if(receivingIndex < receivingBufferList.size() && flushingIndex < flushingBufferList.size()){
                ICachedMNode node1 = receivingBufferList.get(receivingIndex);
                ICachedMNode node2 = flushingBufferList.get(flushingIndex);
                if(node1.getName().compareTo(node2.getName()) < 0){
                    receivingIndex++;
                    return node1;
                }else if(node1.getName().compareTo(node2.getName()) > 0){
                    flushingIndex++;
                    return node2;
                }else{
                    receivingIndex++;
                    flushingIndex++;
                    return node1;
                }
            }
            if(receivingIndex < receivingBufferList.size()){
                return receivingBufferList.get(receivingIndex++);
            }
            if(flushingIndex < flushingBufferList.size()){
                return flushingBufferList.get(flushingIndex++);
            }
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return flushingIndex < flushingBufferList.size() || receivingIndex < receivingBufferList.size();
        }

        @Override
        public ICachedMNode next() {
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            return tryGetNext();
        }
    }
}
