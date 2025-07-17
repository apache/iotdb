package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.Serializable;
import java.util.Arrays;

public class GKBandForDupli {
  public double rankAccuracy;
  private ObjectArrayList<Tuple> entries;
  private int maxEntryNum, entryNum;

  private final double[] buffer;
  private int maxBufferNum, bufferNum;
  private long N;

  public GKBandForDupli(int maxMemoryByte) {
    double pp = 11.0 / 2.0;
    maxEntryNum = (int) Math.floor(maxMemoryByte / (8 * 3 + 8 / pp));
    //    this.rankAccuracy = 11.0/2/maxEntryNum;
    this.rankAccuracy = pp / maxEntryNum; //  |entries|<=11/(2ϵ)log_2(2ϵN)
    maxBufferNum = (int) Math.floor(1 / (2 * rankAccuracy)); // |buffer| = [ 1/(2ε) ]
    this.entries = new ObjectArrayList<>(maxEntryNum);
    this.buffer = new double[maxBufferNum];
    this.bufferNum = 0;
    this.N = 0;
    //    System.out.println("\t\t\t??!!"+1.0*(maxBufferNum*8+maxEntryNum*24)/maxMemoryByte);
  }

  public long getN() {
    compressIfNecessary();
    return N;
  }

  public void update(double value) {
    buffer[bufferNum++] = value;
    if (bufferNum == maxBufferNum) {
      compress();
    }
  }

  public boolean isEmpty() {
    return entries.isEmpty() && bufferNum == 0;
  }

  public double getQuantile(double phi) {
    if (isEmpty()) {
      throw new ArithmeticException();
    }
    compressIfNecessary();

    long maxGD = 0;
    for (Tuple entry : entries) maxGD = Math.max(maxGD, entry.g + entry.delta);
    long targetErr = maxGD / 2;
    final long rank = (long) (phi * (N - 1)) + 1;
    long gSum = 0, maxRank;
    for (Tuple entry : entries) {
      gSum += entry.g;
      maxRank = gSum + entry.delta;
      if (maxRank - targetErr <= rank && rank <= gSum + targetErr) {
        return entry.v;
      }
    }
    return entries.get(entries.size() - 1).v;
  }

  public DoubleArrayList query(DoubleArrayList phis) {
    compressIfNecessary();

    long maxGD = 0;
    for (Tuple entry : entries) maxGD = Math.max(maxGD, entry.g + entry.delta);
    long targetErr = maxGD / 2;
    LongArrayList ranks = new LongArrayList();
    for (double phi : phis) ranks.add((long) (phi * (N - 1)) + 1);
    DoubleArrayList ans = new DoubleArrayList();
    long gSum = 0, maxRank;
    int pos = 0;
    for (Tuple entry : entries) {
      gSum += entry.g;
      maxRank = gSum + entry.delta;
      while (pos < ranks.size()
          && maxRank - targetErr <= ranks.getLong(pos)
          && ranks.getLong(pos) <= gSum + targetErr) {
        ans.add(entry.v);
        pos++;
      }
    }
    while (pos < ranks.size()) {
      ans.add(entries.get(entries.size() - 1).v);
      pos++;
    }
    return ans;
  }

  private void compressIfNecessary() {
    if (bufferNum > 0) {
      compress();
    }
  }

  private void compressStage1() {
    Arrays.sort(buffer, 0, bufferNum);

    final ObjectArrayList<Tuple> mergedEntries = new ObjectArrayList<>(entryNum + bufferNum);
    int pe = entryNum - 1, pb = bufferNum - 1;
    while (pe >= 0 || pb >= 0) {
      if (pb < 0 || (pe >= 0 && entries.get(pe).v > buffer[pb])) {
        mergedEntries.add(entries.get(pe));
        pe--;
      } else {
        N++;
        long delta =
            (pb == 0 && pe < 0) || (pe == entryNum - 1 && pb == bufferNum - 1)
                ? 0
                : (long) Math.floor(2 * rankAccuracy * N);
        mergedEntries.add(new Tuple(buffer[pb], 1, delta));
        pb--;
      }
    }
    entries.clear();
    for (int i = mergedEntries.size() - 1; i >= 0; i--) entries.add(mergedEntries.get(i));
    entryNum = entries.size();
    bufferNum = 0;
  }

  private void compressStage2() {
    final double removalThreshold = (2 * rankAccuracy * N);
    //    System.out.print("\tbeforeCompress\tval(g+delta):\t");
    //    for(Tuple entry:entries)System.out.print("\t"+entry.v+"("+(entry.g+entry.delta)+")");
    //    System.out.println();

    Tuple last = entries.get(entryNum - 1), now;
    final ObjectArrayList<Tuple> revEntries = new ObjectArrayList<>(entryNum + bufferNum);
    for (int i = entryNum - 2; i >= 1; i--) {
      now = entries.get(i);
      if (now.g + last.g + last.delta < removalThreshold
          && now.delta >= last.delta) // band: compare log(2εN-delta) is to compare delta..
      last.g += now.g;
      else {
        revEntries.add(last);
        last = now;
      }
    }
    revEntries.add(last);
    if (entries.get(0).v <= last.v && entryNum > 1) revEntries.add(entries.get(0));

    entries.clear();
    for (int i = revEntries.size() - 1; i >= 0; i--) entries.add(revEntries.get(i));
    entryNum = entries.size();
  }

  private void compress() { // |entries|<=11/(2ϵ)log_2(2ϵN) ; We may need to increase ε to make sure
    // |entries| is limited.
    //    System.out.print("\t\t"+rankAccuracy+"\t\t-->\t\t");
    //    while(true){
    //      int
    // entryNumUpperBound=(int)(11.0/(2*rankAccuracy)*Math.log(2*rankAccuracy*(N+bufferNum))/Math.log(2));
    //      if(entryNumUpperBound>maxEntryNum)
    //        rankAccuracy*=1.05;
    //      else break;
    //    }
    //
    // System.out.println(rankAccuracy+"\t\tentryNumUpperBound:\t"+(int)(11.0/(2*rankAccuracy)*Math.log(2*rankAccuracy*(N+bufferNum))/Math.log(2)));
    //    maxBufferNum=(int)Math.floor(1/(2*rankAccuracy)); // maxBufferNum will decrease as N grows

    //    N += bufferNum;
    compressStage1();

    if (entryNum <= maxEntryNum) return;
    compressStage2();

    //    show();
    if (1.0 * entryNum / maxEntryNum >= 1.0) {
      //      show();
      //      System.out.println("N:" + N + " entry:" + entryNum + "\t" + 1.0 * entryNum /
      // maxEntryNum);
      //      System.out.print("N:" + N + " entry:" + entryNum + "\t" + 1.0 * entryNum / maxEntryNum
      // + "\tThreshold:\t" + removalThreshold + "\t\tval(g+delta,cap,band):\t");
      //      for (Tuple entry : entries) System.out.print("\t" + entry.v + "(" + (entry.g +
      // entry.delta)
      // +","+(removalThreshold-entry.delta)+","+(int)Math.floor(Math.log(removalThreshold-entry.delta+1)/Math.log(2)) + ")");
      //      System.out.println();
      rankAccuracy *= 1.1;
      compress();
    }
  }

  public void show() {
    System.out.print(
        "N:"
            + N
            + "\tε:"
            + rankAccuracy
            + "\tentry:"
            + entryNum
            + "\tfull:"
            + 1.0 * entryNum / maxEntryNum
            + "\t\tval(g+delta,cap,band):\t");
    int removalThreshold = (int) (2 * rankAccuracy * N);
    for (Tuple entry : entries)
      System.out.print(
          "\t"
              + entry.v
              + "("
              + (entry.g + entry.delta)
              + ","
              + (removalThreshold - entry.delta)
              + ","
              + (int) Math.floor(Math.log(removalThreshold - entry.delta + 1) / Math.log(2))
              + ")");

    System.out.println();
  }

  public double[] getFilterL(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + N) return new double[] {valR, -233.0};
    K -= CountOfValL;
    double lb = entries.get(0).v; // =minValueWithRank(K-1);

    long gSum = 0, maxRank;
    for (Tuple entry : entries) {
      gSum += entry.g;
      maxRank = gSum + entry.delta;
      if (maxRank >= K) {
        break;
      }
      lb = entry.v;
    }
    return new double[] {lb};
  }

  public double[] getFilterR(long CountOfValL, long CountOfValR, double valL, double valR, long K) {
    if (K <= CountOfValL) return new double[] {valL, -233.0};
    if (K > CountOfValL + N) return new double[] {valR, -233.0};
    K -= CountOfValL;
    double ub = entries.get(entries.size() - 1).v; // maxValueWithRank(K);
    long gSum = 0, maxRank;
    for (Tuple entry : entries) {
      gSum += entry.g;
      maxRank = gSum + entry.delta;
      if (gSum >= K) {
        ub = entry.v;
        break;
      }
    }
    return new double[] {ub};
  }

  public double[] getFilter(
      long CountOfValL, long CountOfValR, double valL, double valR, long K1, long K2) {
    compressIfNecessary();
    double[] filterL = getFilterL(CountOfValL, CountOfValR, valL, valR, K1);
    double[] filterR = getFilterR(CountOfValL, CountOfValR, valL, valR, K2);
    //    System.out.println("\t\t\t\tvalL,R:\t"+filterL[0]+"..."+filterR[0]);
    if (filterL.length + filterR.length == 4) return new double[] {filterL[0], filterR[0], -233};
    else return new double[] {filterL[0], filterR[0]};
  }

  public long findMaxNumberInRange(double L, double R) { // L,R is value of entry
    compressIfNecessary();
    long gSum = 0, maxRank;
    long LMinRank = 1, RMaxRank = N;
    for (Tuple entry : entries) {
      gSum += entry.g;
      maxRank = gSum + entry.delta;
      if (entry.v == L) LMinRank = gSum;
      if (entry.v == R) RMaxRank = maxRank;
    }
    return RMaxRank - LMinRank + 1;
  }

  static class Tuple implements Serializable {
    private final double v;
    public long g;
    public long delta;

    private Tuple(double v, long g, long delta) {
      this.v = v;
      this.g = g;
      this.delta = delta;
    }
  }
}
