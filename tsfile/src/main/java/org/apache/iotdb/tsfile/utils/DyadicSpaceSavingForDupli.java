package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class DyadicSpaceSavingForDupli {
  final int LOG_U = 64, BytePerItem = Long.BYTES * 2;
  private long N;
  private int maxItemNum,
      exactTopLayer,
      botLayerMaxItem; // [exactTopLayer , LOG_U) are exact layers.
  Long2LongOpenHashMap[] layerV2C = new Long2LongOpenHashMap[LOG_U];
  ObjectArrayList<Long2ObjectAVLTreeMap<LongOpenHashSet>> layerC2Vs = new ObjectArrayList<>(LOG_U);

  private long dataToLong(double data) {
    long result = Double.doubleToLongBits((double) data);
    return data >= 0d ? result : result ^ Long.MAX_VALUE;
  }

  private double longToResult(long result) {
    result = (result >>> 63) == 0 ? result : result ^ Long.MAX_VALUE;
    return Double.longBitsToDouble(result);
  }

  void calcExactTopLayer() {
    exactTopLayer = LOG_U;
    for (int i = LOG_U - 1, tmpTopSum = 0; i >= 0; i--) {
      int cntLayer = (int) (1 << (LOG_U - i)), botAvg = (maxItemNum - tmpTopSum) / i;
      tmpTopSum += 1 << (LOG_U - i);
      if (cntLayer >= botAvg) {
        exactTopLayer = i + 1;
        break;
      }
    }
  }

  public DyadicSpaceSavingForDupli(int maxSerialByte) {
    this.N = 0;
    this.maxItemNum = maxSerialByte / BytePerItem;
    this.calcExactTopLayer();
    this.botLayerMaxItem = (maxItemNum - (int) (1 << (LOG_U - exactTopLayer)) + 1) / exactTopLayer;
    for (int i = 0; i < LOG_U; i++) {
      layerV2C[i] = new Long2LongOpenHashMap();
      layerC2Vs.add(new Long2ObjectAVLTreeMap<LongOpenHashSet>());
    }
    //    System.out.println("\t\tInitial DSS.\t
    // maxItemNum:"+maxItemNum+"\texact_layer:"+(LOG_U-exactTopLayer)+"\tbotLayerMaxItem:"+botLayerMaxItem);
  }

  public long getN() {
    return N;
  }

  public void update(double x) {
    N++;
    long lx = dataToLong(x);
    long unsignedX = lx ^ (1L << 63);
    for (int i = 0; i < LOG_U; i++) {
      updateSpaceSaving(layerV2C[i], layerC2Vs.get(i), unsignedX);
      unsignedX = unsignedX >>> 1;
    }
  }

  void updateSpaceSaving(
      Long2LongOpenHashMap mapV2C, Long2ObjectAVLTreeMap<LongOpenHashSet> mapC2Vs, long v) {
    long existingC = mapV2C.getOrDefault(v, 0);
    if (existingC > 0) {
      mapV2C.addTo(v, 1);
      mapC2Vs.get(existingC).remove(v);
      if (mapC2Vs.get(existingC).isEmpty()) mapC2Vs.remove(existingC);
      if (!mapC2Vs.containsKey(existingC + 1)) mapC2Vs.put(existingC + 1, new LongOpenHashSet());
      mapC2Vs.get(existingC + 1).add(v);
    } else {
      if (mapV2C.size() >= botLayerMaxItem) {
        long minC = mapC2Vs.firstLongKey();
        long replacedV = mapC2Vs.get(minC).longIterator().nextLong();
        mapV2C.remove(replacedV);
        mapV2C.put(v, minC + 1);
        mapC2Vs.get(minC).remove(replacedV);
        if (mapC2Vs.get(minC).isEmpty()) mapC2Vs.remove(minC);
        if (!mapC2Vs.containsKey(minC + 1)) mapC2Vs.put(minC + 1, new LongOpenHashSet());
        mapC2Vs.get(minC + 1).add(v);
      } else {
        mapV2C.put(v, 1);
        if (!mapC2Vs.containsKey(1)) mapC2Vs.put(1, new LongOpenHashSet());
        mapC2Vs.get(1).add(v);
      }
    }
  }

  private long getRank(long x) {
    long unsignedX = x ^ (1L << 63);
    long rank = layerV2C[0].getOrDefault(unsignedX, 0); // +layerV2C[0].getOrDefault(lx-1,0);
    for (int i = 0; i < LOG_U; i++) {
      if ((unsignedX & 1) == 1) rank += layerV2C[i].getOrDefault(unsignedX ^ 1, 0);
      unsignedX >>>= 1;
    }
    return rank;
  }

  public double getQuantile(double q) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE;
    //
    // System.out.println("\t\t"+dataToLong(-1.0)+"\t"+dataToLong(1.0)+"\t"+(+dataToLong(-1.0)<dataToLong(1.0))+"\t"+Double.doubleToLongBits(-1)+"\t"+Double.doubleToLongBits(1));
    while (L < R) {
      long mid = L / 2 + R / 2;
      if (L + 2 >= R) mid = L;
      //      System.out.println("\t\tBinary
      // Search\t"+L+"\t"+R+"\t"+mid+"\t\tvalid:"+(L<=mid&&mid<=R)+"\tmidV:"+(longToResult(mid)));
      if (getRank(mid) >= q * N) R = mid;
      else L = mid + 1;
    }
    return longToResult(L);
  }
}
