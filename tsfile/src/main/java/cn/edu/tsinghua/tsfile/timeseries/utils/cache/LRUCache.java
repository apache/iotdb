package cn.edu.tsinghua.tsfile.timeseries.utils.cache;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public abstract class LRUCache<K, T> implements Cache<K, T> {

    private int cacheSize;
    private Map<K, T> cache;

    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.cache = new LinkedHashMap<>();
    }

    @Override
    public T get(K key) throws CacheException {
        if (cache.containsKey(key)) {
            moveObjectToTail(key);
        } else {
            removeFirstObjectIfCacheIsFull();
            cache.put(key, loadObjectByKey(key));
        }
        return cache.get(key);
    }

    public void clear() {
        this.cache.clear();
    }

    private void moveObjectToTail(K key) {
        T value = cache.get(key);
        cache.remove(key);
        cache.put(key, value);
    }

    private void removeFirstObjectIfCacheIsFull() throws CacheException {
        if (cache.size() == this.cacheSize) {
            removeFirstObject();
        }
    }

    private void removeFirstObject() throws CacheException {
        if (cache.size() == 0) {
            return;
        }
        K key = cache.keySet().iterator().next();
        beforeRemove(cache.get(key));
        cache.remove(key);
    }

    /**
     * Do something before remove object from cache.
     *
     * @param object
     */
    public abstract void beforeRemove(T object) throws CacheException;

    public abstract T loadObjectByKey(K key) throws CacheException;
}
