package cn.edu.tsinghua.tsfile.common.cache;

import cn.edu.tsinghua.tsfile.exception.cache.CacheException;

import java.io.IOException;


public interface Cache<K, T> {
    T get(K key) throws CacheException, IOException;

    void clear();
}
