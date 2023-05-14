package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SegTreeBySketch {
  int level, leafNum; // ,sizeRatio;
  public LongArrayList queriedChunkL, queriedChunkR;
  public ObjectArrayList<LongArrayList> sketchMinT, sketchMaxT;
  public ObjectArrayList<ObjectArrayList<KLLSketchForQuantile>> sketch;
  public static final int LSM_T = 30;

  public void show() {
    System.out.println("\t\tshowing a Seg Tree sketches for an SSTable.\tlevel=" + level);
    for (int i = 1; i <= level; i++)
      System.out.print("\t\t[ " + sketch.get(i).size() + " sketches ]");
    System.out.println();
  }

  public SegTreeBySketch(
      ObjectArrayList<KLLSketchForQuantile> leaves,
      LongArrayList leafMinT,
      LongArrayList leafMaxT,
      int sketch_size_ratio) {
    leafNum = leaves.size();
    //    sizeRatio = sketch_size_ratio;
    int tmpNum = leaves.size();
    level = 0;
    sketch = new ObjectArrayList<>();
    sketch.add(new ObjectArrayList<>());
    sketchMinT = new ObjectArrayList<>();
    sketchMinT.add(new LongArrayList());
    sketchMaxT = new ObjectArrayList<>();
    sketchMaxT.add(new LongArrayList());
    while (tmpNum >= LSM_T) {
      level++;
      sketch.add(new ObjectArrayList<>());
      sketchMinT.add(new LongArrayList());
      sketchMaxT.add(new LongArrayList());
      for (int i = 0; i < tmpNum / LSM_T; i++) {
        KLLSketchForSST segSketch = new KLLSketchForSST();
        for (int j = 0; j < LSM_T; j++)
          segSketch.addSubSketch(
              level == 1 ? leaves.get(i * LSM_T + j) : sketch.get(level - 1).get(i * LSM_T + j));
        segSketch.compactSubSketches(sketch_size_ratio);
        sketch.get(level).add(segSketch);
        sketchMinT
            .get(level)
            .add(
                level == 1
                    ? leafMinT.getLong(i * LSM_T)
                    : sketchMinT.get(level - 1).getLong(i * LSM_T));
        sketchMaxT
            .get(level)
            .add(
                level == 1
                    ? leafMaxT.getLong(i * LSM_T + LSM_T - 1)
                    : sketchMaxT.get(level - 1).getLong(i * LSM_T + LSM_T - 1));
      }
      tmpNum /= LSM_T;
    }
  }

  public int serializeSegTree(OutputStream outputStream) throws IOException {
    //    System.out.println("\t\t\t\t\t[DEBUG DOUBLE stat] serializeStats
    // hashmap:"+serializeHashMap);
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(level, outputStream);
    byteLen += ReadWriteIOUtils.write(leafNum, outputStream); // how many chunks
    //    byteLen += ReadWriteIOUtils.write(sizeRatio, outputStream);

    for (int i = 1; i <= level; i++) {
      for (int j = 0; j < sketch.get(i).size(); j++) {
        byteLen += ReadWriteIOUtils.write(sketchMinT.get(i).getLong(j), outputStream);
        byteLen += ReadWriteIOUtils.write(sketchMaxT.get(i).getLong(j), outputStream);
        byteLen += (new LongKLLSketch(sketch.get(i).get(j))).serialize(outputStream);
      }
    }
    return byteLen;
  }

  public SegTreeBySketch(ByteBuffer byteBuffer) {
    level = ReadWriteIOUtils.readInt(byteBuffer);
    leafNum = ReadWriteIOUtils.readInt(byteBuffer);
    //    sizeRatio = ReadWriteIOUtils.readInt(byteBuffer);

    sketch = new ObjectArrayList<>();
    sketch.add(new ObjectArrayList<>());
    sketchMinT = new ObjectArrayList<>();
    sketchMinT.add(new LongArrayList());
    sketchMaxT = new ObjectArrayList<>();
    sketchMaxT.add(new LongArrayList());

    for (int i = 1, tmpNum = leafNum; i <= level; i++) {
      sketch.add(new ObjectArrayList<>());
      sketchMinT.add(new LongArrayList());
      sketchMaxT.add(new LongArrayList());
      for (int j = 0; j < tmpNum / LSM_T; j++) {
        sketchMinT.get(i).add(ReadWriteIOUtils.readLong(byteBuffer));
        sketchMaxT.get(i).add(ReadWriteIOUtils.readLong(byteBuffer));
        sketch.get(i).add(new LongKLLSketch(byteBuffer));
      }
      tmpNum /= LSM_T;
    }
  }

  private boolean inInterval(long x, long y, long L, long R) {
    return x >= L && y <= R;
  }

  private boolean inInterval(long x, long L, long R) {
    return x >= L && x <= R;
  }

  private boolean overlapInterval(long x, long y, long L, long R) { // [L,R]
    return !(y < L || x > R);
  }

  private boolean timeFilter_contains(Filter timeFilter, long L, long R) {
    return timeFilter == null || timeFilter.containStartEndTime(L, R);
  }

  // start from a node without father in this SegTree
  private void range_query_in_node(
      int lv,
      int p,
      ObjectArrayList<KLLSketchForQuantile> queriedSketch,
      Filter timeFilter,
      long otherL,
      long otherR,
      ObjectArrayList<ITimeSeriesMetadata> overlappedTSMD) {
    // other SST may partially overlap with this SSTable. only [otherL,otherR] in  this SST don't
    // overlap.
    if (lv <= 0) return;
    long cntL = sketchMinT.get(lv).getLong(p), cntR = sketchMaxT.get(lv).getLong(p);
    //    if (!timeFilter.o overlapInterval(cntL, cntR, L, R)) return;
    System.out.println(
        "\t\t\t\t\tcnt seg node:"
            + "lv="
            + lv
            + cntL
            + "..."
            + cntR
            + "\t\t\t\tqueryTimeFilter:"
            + timeFilter
            + "\t\tcontainedInQuery:"
            + timeFilter_contains(timeFilter, cntL, cntR)
            + "\t\tcontainedInQuery:"
            + timeFilter_contains(timeFilter, cntL, cntR));
    if (timeFilter_contains(timeFilter, cntL, cntR) && inInterval(cntL, cntR, otherL, otherR)) {
      boolean node_non_overlap = true;
      for (ITimeSeriesMetadata tsmd : overlappedTSMD)
        if (overlapInterval(
            cntL, cntR, tsmd.getStatistics().getStartTime(), tsmd.getStatistics().getEndTime()))
          node_non_overlap = false;
      if (node_non_overlap) {
        //            System.out.println("\t\tmerge with
        // T:"+cntL+"..."+cntR+"\t\t\tlv="+lv+"\t\tcntN:"+query_sketch.getN());
        ((LongKLLSketch) sketch.get(lv).get(p)).deserializeFromBuffer();
        queriedSketch.add(sketch.get(lv).get(p));

        int weight = 1;
        for (int i = 1; i <= lv; i++) weight *= LSM_T;
        System.out.println(
            "\t\t\tmerge with the "
                + (p * weight)
                + "..."
                + (p * weight + weight - 1)
                + " chunks in SST sketches.");
        queriedChunkL.add(sketchMinT.get(lv).getLong(p));
        queriedChunkR.add(sketchMaxT.get(lv).getLong(p));
        return;
      }
    }
    for (int i = 0; i < LSM_T; i++)
      range_query_in_node(
          lv - 1, p * LSM_T + i, queriedSketch, timeFilter, otherL, otherR, overlappedTSMD);
  }

  public void range_query_in_SST_sketches(
      ObjectArrayList<KLLSketchForQuantile> queriesSketch,
      Filter timeFilter,
      long otherL,
      long otherR,
      ObjectArrayList<ITimeSeriesMetadata>
          overlappedTSMD) { // other SST may partially overlap with this SSTable. only
    // [otherL,otherR] in
    // this SST don't overlap.
    queriedChunkL = new LongArrayList();
    queriedChunkR = new LongArrayList();
    int last = 0;
    for (int i = level; i >= 1; i--) {
      for (int j = last * LSM_T; j < sketch.get(i).size(); j++) {
        //        if (timeFilter.containStartEndTime(
        //                sketchMinT.get(i).getLong(j), sketchMaxT.get(i).getLong(j))
        //            && overlapInterval(
        //                sketchMinT.get(i).getLong(j), sketchMaxT.get(i).getLong(j), otherL,
        // otherR))
        range_query_in_node(i, j, queriesSketch, timeFilter, otherL, otherR, overlappedTSMD);
      }
      last = sketch.get(i).size();
    }
  }
}
