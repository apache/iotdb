package org.apache.iotdb.tsfile.common.cache;

import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.exception.cache.CacheException;

import java.io.IOException;


public interface Cache<K, T> {
    T get(K key) throws CacheException, IOException;

    void clear();
}
