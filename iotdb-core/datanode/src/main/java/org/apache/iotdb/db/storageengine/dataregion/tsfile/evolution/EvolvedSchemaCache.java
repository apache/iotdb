package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.fileset.TsFileSet;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;

import java.util.function.Supplier;

public class EvolvedSchemaCache {

  private final Cache<TsFileSet, EvolvedSchema> cache;

  private EvolvedSchemaCache() {
    cache =
        Caffeine.newBuilder()
            .weigher(
                (Weigher<TsFileSet, EvolvedSchema>)
                    (k, v) -> {
                      // TsFileSet is always in memory, do not count it
                      return (int) v.ramBytesUsed();
                    })
            .maximumWeight(
                // TODO-Sevo configurable
                128 * 1024 * 1024L)
            .build();
  }

  public EvolvedSchema computeIfAbsent(
      TsFileSet tsFileSet, Supplier<EvolvedSchema> schemaSupplier) {
    return cache.get(tsFileSet, k -> schemaSupplier.get());
  }

  public void invalidate(TsFileSet tsFileSet) {
    cache.invalidate(tsFileSet);
  }

  public static EvolvedSchemaCache getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private static final EvolvedSchemaCache INSTANCE = new EvolvedSchemaCache();
  }
}
