package org.apache.iotdb.db.auth;

import org.apache.iotdb.commons.auth.entity.User;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class author {

  @Test
  public void test() {
    LoadingCache<String, User> loadingCache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(30, TimeUnit.SECONDS)
            // getAll将会对缓存中, 没有值的key分别调用CacheLoader.load方法来构建缓存的值
            .build(this::buildLoader);
    // 存储要添加的缓存
    List<String> keys = new ArrayList<>();
    keys.add("cl");
    keys.add("b");
    keys.add("c");
    keys.add("d");
    // 返回一个map
    Map<String, User> cacheAll = loadingCache.getAll(keys);
    System.out.println(cacheAll.get("b"));
    System.out.println(cacheAll.get("c"));
    System.out.println(cacheAll.get("a"));
  }

  User buildLoader(String username) {
    User user = new User();
    user.setName(username);
    return user;
  }
}
