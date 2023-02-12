package org.apache.iotdb.db.metadata.cache.dualkeycache.impl;

import java.util.function.Function;

class CacheSizeComputerImpl<FK, SK, V> implements ICacheSizeComputer<FK, SK, V> {

  private final Function<FK, Integer> firstKeySizeComputer;

  private final Function<SK, Integer> secondKeySizeComputer;

  private final Function<V, Integer> valueSizeComputer;

  CacheSizeComputerImpl(
      Function<FK, Integer> firstKeySizeComputer,
      Function<SK, Integer> secondKeySizeComputer,
      Function<V, Integer> valueSizeCompute) {
    this.firstKeySizeComputer = firstKeySizeComputer;
    this.secondKeySizeComputer = secondKeySizeComputer;
    this.valueSizeComputer = valueSizeCompute;
  }

  @Override
  public int computeFirstKeySize(FK firstKey) {
    return firstKeySizeComputer.apply(firstKey);
  }

  @Override
  public int computeSecondKeySize(SK secondKey) {
    return secondKeySizeComputer.apply(secondKey);
  }

  @Override
  public int computeValueSize(V value) {
    return valueSizeComputer.apply(value);
  }
}
