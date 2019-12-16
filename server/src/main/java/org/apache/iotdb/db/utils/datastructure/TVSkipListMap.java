package org.apache.iotdb.db.utils.datastructure;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @Author: LiuDaWei
 * @Create: 2019年12月16日
 */
public class TVSkipListMap<K, V> extends ConcurrentSkipListMap<K, V> {

  private long timeOffset;

  public TVSkipListMap(Comparator<? super K> comparable) {
    super(comparable);
  }

  public long getTimeOffset() {
    return timeOffset;
  }

  public void setTimeOffset(long timeOffset) {
    this.timeOffset = timeOffset;
  }
}
