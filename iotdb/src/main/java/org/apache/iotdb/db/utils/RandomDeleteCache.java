package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.common.cache.Cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public abstract class RandomDeleteCache<K, V> implements Cache<K, V> {

    private int cacheSize;
    private Map<K, V> cache;

    public RandomDeleteCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public V get(K key) throws CacheException {
        V v = cache.get(key);
        if (v == null) {
            randomRemoveObjectIfCacheIsFull();
            cache.put(key, loadObjectByKey(key));
            v = cache.get(key);
        }
        return v;
    }

    private void randomRemoveObjectIfCacheIsFull() throws CacheException {
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
    public abstract void beforeRemove(V object) throws CacheException;

    public abstract V loadObjectByKey(K key) throws CacheException;

    @Override
    public void clear() {
        cache.clear();
    }

    public int size() {
        return cache.size();
    }
}
