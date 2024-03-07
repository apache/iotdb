package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.tsfile.utils.Pair;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.util.List;

public class EnrichedTabletsBinaryCache {

  private final PipeMemoryBlock allocatedMemoryBlock;

  private final LoadingCache<Long, Pair<ByteBuffer, List<EnrichedEvent>>> cache;

  public EnrichedTabletsBinaryCache() {
    // TODO: config
    this.allocatedMemoryBlock =
        PipeResourceManager.memory().tryAllocate(Runtime.getRuntime().maxMemory() / 50);
    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(this.allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<Long, Pair<ByteBuffer, List<EnrichedEvent>>>)
                    (id, enrichedTablets) -> enrichedTablets.left.limit())
            .build(
                new CacheLoader<Long, Pair<ByteBuffer, List<EnrichedEvent>>>() {
                  @Override
                  public @Nullable Pair<ByteBuffer, List<EnrichedEvent>> load(@NonNull Long aLong)
                      throws Exception {
                    return null;
                  }
                });
  }
}
