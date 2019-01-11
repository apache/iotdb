package cn.edu.tsinghua.tsfile.common.cache;


import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is not thread safe.
 *
 * @param <K>
 * @param <T>
 */
public abstract class LRUCache<K, T> implements Cache<K, T> {

    private int cacheSize;
    private Map<K, T> cache;

    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.cache = new LinkedHashMap<>();
    }

    @Override
    public T get(K key) throws IOException {
        if (cache.containsKey(key)) {
            moveObjectToTail(key);
        } else {
            removeFirstObjectIfCacheIsFull();
            cache.put(key, loadObjectByKey(key));
        }
        return cache.get(key);
    }

    @Override
    public void clear() {
        this.cache.clear();
    }

    private void moveObjectToTail(K key) {
        T value = cache.get(key);
        cache.remove(key);
        cache.put(key, value);
    }

    private void removeFirstObjectIfCacheIsFull() {
        if (cache.size() == this.cacheSize) {
            removeFirstObject();
        }
    }

    private void removeFirstObject() {
        if (cache.size() == 0) {
            return;
        }
        K key = cache.keySet().iterator().next();
        cache.remove(key);
    }


    public void put(K key, T value) {
        cache.remove(key);
        removeFirstObjectIfCacheIsFull();
        cache.put(key, value);
    }

    public abstract T loadObjectByKey(K key) throws IOException;

}
