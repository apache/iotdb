/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

import com.google.common.hash.BloomFilter;
import it.unimi.dsi.fastutil.Stack;
import it.unimi.dsi.fastutil.longs.Long2ReferenceAVLTreeMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.*;

public class SeriesReaderForStat extends SeriesReader {

  public boolean tryToUseBF = false;
  public boolean usingBF = false;
  public boolean readingOldPages = false;
  public long lastInitTime = Long.MIN_VALUE;

  public SeriesReaderForStat(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter) {
    super(
        seriesPath,
        allSensors,
        dataType,
        context,
        dataSource,
        timeFilter,
        valueFilter,
        fileFilter,
        true);
  }

  private boolean checkPointInRange(long p, long l, long r) {
    return l <= p && p <= r;
  }

  private boolean checkRangeOverlap(long l1, long r1, long l2, long r2) {
    return !(r1 < l2 || r2 < l1);
  }

  private boolean checkChunkMdOverlap(IChunkMetadata x, IChunkMetadata y) {
    return checkRangeOverlap(x.getStartTime(), x.getEndTime(), y.getStartTime(), y.getEndTime());
  }

  private boolean checkChunkMdOverlap(IChunkMetadata x, long l, long r) {
    return checkRangeOverlap(x.getStartTime(), x.getEndTime(), l, r);
  }

  private boolean checkChunkMdOverlap(IChunkMetadata x, List<IChunkMetadata> others) {
    for (IChunkMetadata y : others) if (checkChunkMdOverlap(x, y)) return true;
    return false;
  }

  private boolean checkBFOverlapChunkMd(BF bf, IChunkMetadata x) {
    return checkRangeOverlap(bf.minTime, bf.maxTime, x.getStartTime(), x.getEndTime());
  }

  private boolean checkChunkMdContains(IChunkMetadata x, long l, long r) {
    return x.getStartTime() <= l && x.getEndTime() >= r;
  }

  private long getMaxTimeInChunkMdList(List<IChunkMetadata> list) {
    long mx = Long.MIN_VALUE;
    for (IChunkMetadata x : list) mx = Math.max(mx, x.getEndTime());
    return mx;
  }

  private long getMaxTimeInChunkMdSet(Set<IChunkMetadata> set) {
    long mx = Long.MIN_VALUE;
    for (IChunkMetadata x : set) mx = Math.max(mx, x.getEndTime());
    return mx;
  }

  private long getMinTimeInChunkMdList(List<IChunkMetadata> list) {
    long mn = Long.MAX_VALUE;
    for (IChunkMetadata x : list) mn = Math.min(mn, x.getStartTime());
    return mn;
  }

  private long getMinTimeInChunkMdSet(Set<IChunkMetadata> set) {
    long mn = Long.MAX_VALUE;
    for (IChunkMetadata x : set) mn = Math.min(mn, x.getStartTime());
    return mn;
  }

  Reference2ReferenceOpenHashMap<IChunkMetadata, List<IPageReader>> loadedChunkMd;
  ObjectAVLTreeSet<IChunkMetadata> overlappedChunkMd;
  Reference2ReferenceOpenHashMap<IChunkMetadata, ReferenceOpenHashSet<IChunkMetadata>>
      chunkNeighbor;
  IChunkMetadata oldestChunkMd;

  //  private class ChunkMdList{
  //    public List<IChunkMetadata> list;
  //    public ChunkMdList(List<IChunkMetadata> list){
  //      this.list=list;
  //    }
  //  }
  private class BF {
    public BloomFilter<Long> bf;
    public long minTime, maxTime;
    public IChunkMetadata chunkMd;
    public int bfId;

    public BF(IChunkMetadata chunkMd, int bfId) {
      this.bf = chunkMd.getStatistics().getBf(bfId);
      this.minTime = chunkMd.getStatistics().getBfMinTime(bfId);
      this.maxTime = chunkMd.getStatistics().getBfMaxTime(bfId);
      this.chunkMd = chunkMd;
      this.bfId = bfId;
    }

    public BF(BloomFilter<Long> bf, long minT, long maxT, IChunkMetadata chunkMd, int bfId) {
      this.bf = bf;
      this.minTime = minT;
      this.maxTime = maxT;
      this.chunkMd = chunkMd;
      this.bfId = bfId;
    }
  }

  private List<IPageReader> getPageReadersFromChunkMd(IChunkMetadata chunkMd) throws IOException {
    if (loadedChunkMd.containsKey(chunkMd)) {
      //      System.out.println("\t\t\tdebug\tloadedChunk hit!\tminT:" +
      //          chunkMd.getStartTime());
      return loadedChunkMd.get(chunkMd);
    } else {
      List<IPageReader> result = FileLoaderUtils.loadLazyPageReaderList(chunkMd, timeFilter);
      loadedChunkMd.put(chunkMd, result);
      return result;
    }
  }

  private void debugChunkMd(IChunkMetadata chunkMd) {
    System.out.print("\t" + chunkMd.getStartTime() + "..." + chunkMd.getEndTime());
  }

  private void debugChunkMdSet(Set<IChunkMetadata> chunkMdSet) {
    System.out.print("{");
    for (IChunkMetadata x : chunkMdSet) debugChunkMd(x);
    System.out.print("}");
  }

  private void getSomeOverlapChunkMd() throws IOException {
    long cntEndTime = overlappedChunkMd.last().getEndTime();
    unpackAllOverlappedTsFilesToTimeSeriesMetadata(cntEndTime);
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(cntEndTime, false);
    while (!cachedChunkMetadata.isEmpty()
        && cachedChunkMetadata.first().getStartTime() <= cntEndTime) {
      IChunkMetadata tmp = cachedChunkMetadata.first();
      cachedChunkMetadata.remove(tmp);
      overlappedChunkMd.add(tmp);
    }
    oldestChunkMd = overlappedChunkMd.first();
    unpackAllOverlappedTsFilesToTimeSeriesMetadata(getMaxTimeInChunkMdSet(overlappedChunkMd));
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
        getMaxTimeInChunkMdSet(overlappedChunkMd), false);
    while (!cachedChunkMetadata.isEmpty()
        && cachedChunkMetadata.first().getStartTime() <= oldestChunkMd.getEndTime()) {
      IChunkMetadata tmp = cachedChunkMetadata.first();
      cachedChunkMetadata.remove(tmp);
      overlappedChunkMd.add(tmp);
    }

    System.out.println(
        "\t\t\toldest:" + oldestChunkMd.getStartTime() + ".." + oldestChunkMd.getEndTime());
    for (IChunkMetadata chunkMd : overlappedChunkMd)
      if (!checkChunkMdOverlap(chunkMd, oldestChunkMd)) {
        cachedChunkMetadata.add(chunkMd);
        overlappedChunkMd.remove(chunkMd);
        //        System.out.println(
        //            "\t\t\tremoving:" + chunkMd.getStartTime() +
        //                ".." + chunkMd.getEndTime());
      }

    System.out.print("\t[DEBUG][SeriesReaderForStat getSomeOverlapChunkMd]: ");
    for (IChunkMetadata chunkMd : overlappedChunkMd) debugChunkMd(chunkMd);
    System.out.println();
  }

  private void checkOverlapDetail() {
    Long2ReferenceAVLTreeMap<BF> endTimeToBF = new Long2ReferenceAVLTreeMap<>();
    ObjectArrayList<BF> allBF = new ObjectArrayList<>(overlappedChunkMd.size() * 2);
    long tmpMaxT = Long.MIN_VALUE;
    for (IChunkMetadata chunkMd : overlappedChunkMd) {
      chunkNeighbor.putIfAbsent(chunkMd, new ReferenceOpenHashSet<>(2));
      tmpMaxT = Math.max(tmpMaxT, chunkMd.getEndTime());
      for (int i = 0; i < chunkMd.getStatistics().getBfNum(); i++) allBF.add(new BF(chunkMd, i));
    }
    List<IChunkMetadata> tmpList = new ArrayList<>();
    while (!cachedChunkMetadata.isEmpty()
        && cachedChunkMetadata.first().getStartTime() <= tmpMaxT) {
      IChunkMetadata tmp = cachedChunkMetadata.first();
      cachedChunkMetadata.remove(tmp);
      tmpList.add(tmp);
    }
    for (IChunkMetadata chunkMd : tmpList)
      for (int i = 0; i < chunkMd.getStatistics().getBfNum(); i++) allBF.add(new BF(chunkMd, i));
    cachedChunkMetadata.addAll(tmpList);

    allBF.sort(Comparator.comparingLong(x -> x.maxTime));
    for (BF bf : allBF) {
      for (BF another : endTimeToBF.tailMap(bf.minTime).values()) {
        //        System.out.println(
        //            "\t[DEBUG][SeriesReaderForStat checkOverlapDetail]: "
        //                + " bf overlap:"
        //                + another.minTime
        //                + "..."
        //                + another.maxTime
        //                + " & "
        //                + bf.minTime
        //                + "..."
        //                + bf.maxTime);
        if (overlappedChunkMd.contains(bf.chunkMd))
          chunkNeighbor.get(bf.chunkMd).add(another.chunkMd);
        if (overlappedChunkMd.contains(another.chunkMd))
          chunkNeighbor.get(another.chunkMd).add(bf.chunkMd);
      }
      endTimeToBF.put(bf.maxTime, bf);
    }
    //    for (IChunkMetadata chunkMd : chunkNeighbor.keySet()) {
    //      System.out.println("\t[DEBUG][SeriesReaderForStat checkOverlapDetail]: ");
    //      System.out.print("\t");
    //      debugChunkMd(chunkMd);
    //      System.out.print("\n\t\t");
    //      for (IChunkMetadata another : chunkNeighbor.get(chunkMd)) debugChunkMd(another);
    //      System.out.print("\n");
    //    }
    //    System.out.print("\n");
  }

  private boolean chunkMdVersionSmaller(IChunkMetadata a, IChunkMetadata b) {
    return a.getVersion() != b.getVersion()
        ? a.getVersion() < b.getVersion()
        : a.getOffsetOfChunkHeader() < b.getOffsetOfChunkHeader();
  }

  IChunkMetadata checkingChunkMd;
  ObjectBidirectionalIterator<IChunkMetadata> checkingIt;
  ObjectArrayList<IChunkMetadata> noUpdateChunkMd;
  Stack<BF> noUpdateBF;
  Reference2ReferenceOpenHashMap<IChunkMetadata, ReferenceOpenHashSet<IChunkMetadata>>
      chunkInsideUpdateSet,
      //      chunkUsedToUpdateOutside = new Reference2ReferenceOpenHashMap<>(),
      chunkUsedToNoUpdate = new Reference2ReferenceOpenHashMap<>(),
      chunkUsedToUpdate = new Reference2ReferenceOpenHashMap<>();
  public ReferenceList<IChunkMetadata> boundChunkMd;
  List<IChunkMetadata> cntBlock;

  private void getBoundChunkMd(long cntEndTime) throws IOException {
    // already know no update.
    unpackAllOverlappedTsFilesToTimeSeriesMetadata(cntEndTime);
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(cntEndTime, false);
    while (!cachedChunkMetadata.isEmpty()
        && cachedChunkMetadata.first().getStartTime() <= cntEndTime) {
      boundChunkMd.add(cachedChunkMetadata.first());
      cachedChunkMetadata.remove(cachedChunkMetadata.first());
    }
  }

  private void releaseBoundChunkMd() {
    if (!boundChunkMd.isEmpty()) cachedChunkMetadata.addAll(boundChunkMd);
    boundChunkMd.clear();
  }

  private void mergeUpdateSet(IChunkMetadata x, IChunkMetadata y) {
    ReferenceOpenHashSet<IChunkMetadata> xSet = chunkInsideUpdateSet.get(x);
    ReferenceOpenHashSet<IChunkMetadata> ySet = chunkInsideUpdateSet.get(y);
    if (xSet != null && ySet == xSet) return;
    ReferenceOpenHashSet<IChunkMetadata> mergedSet;
    if (xSet == null && ySet == null) {
      mergedSet = new ReferenceOpenHashSet<>();
      mergedSet.add(x);
      mergedSet.add(y);
    } else if (xSet != null && ySet != null) {
      mergedSet = xSet;
      mergedSet.addAll(ySet);
    } else {
      mergedSet = xSet != null ? xSet : ySet;
      mergedSet.add(xSet != null ? y : x);
    }
    chunkInsideUpdateSet.put(x, mergedSet);
    chunkInsideUpdateSet.put(y, mergedSet);
  }

  private boolean highFPR(IChunkMetadata x, List<IChunkMetadata> others) {
    Statistics xStat = x.getStatistics();
    double noFP = 1.0;
    for (int i = 0; i < xStat.getBfNum(); i++) {
      long timesToCheck = 0;
      for (IChunkMetadata a : others)
        if (checkChunkMdContains(a, xStat.getBfMinTime(i), xStat.getBfMaxTime(i)))
          timesToCheck += xStat.getBfCount(i);
      double fpr = xStat.getBf(i).expectedFpp();
      noFP *= Math.pow(1 - fpr, timesToCheck);
    }
    return noFP < 0.6;
  }

  private ReferenceOpenHashSet<IChunkMetadata> checkMightUpdate(
      IChunkMetadata checkingChunkMd, List<IChunkMetadata> others, boolean outside, boolean prune)
      throws IOException {
    //    System.out.print("\t[DEBUG][checkOntChunkMd find other chunkMds] checking:");
    //    debugChunkMd(checkingChunkMd);
    if (outside) {
      if (chunkUsedToUpdate.containsKey(checkingChunkMd)) {
        ReferenceOpenHashSet<IChunkMetadata> tmp = chunkUsedToUpdate.get(checkingChunkMd);
        for (IChunkMetadata x : others)
          if (tmp.contains(x)) {
            //            System.out.println("\t\tWith a OLD Outside chunk which might updates.");
            return new ReferenceOpenHashSet<>(Collections.singleton(x));
          }
      }
      if (chunkUsedToNoUpdate.containsKey(checkingChunkMd))
        others.removeAll(chunkUsedToNoUpdate.get(checkingChunkMd));
    }
    if (outside && !others.isEmpty() && highFPR(checkingChunkMd, others)) {
      //      System.out.println("\t\t[DEBUG] HighFPR = =\tothers_size:" +
      //          others.size());
      return null;
    }
    ReferenceOpenHashSet<IChunkMetadata> chunkMdMightUpdate = new ReferenceOpenHashSet<>();

    if (!outside && !prune) { // it is an inside check.
      ReferenceOpenHashSet<IChunkMetadata> tmp = chunkUsedToNoUpdate.get(checkingChunkMd);
      if (tmp != null) {
        //        boolean first = true;
        for (IChunkMetadata x : tmp)
          if (others.contains(x)) {
            others.remove(x);
            //            if (first) {
            //              System.out.print("\tOLD_NoUpdated(");
            //              first = false;
            //            }
            //            debugChunkMd(x);
          }
        //        if (!first) System.out.print(")");
      }
      tmp = chunkUsedToUpdate.get(checkingChunkMd);
      if (tmp != null) {
        //        boolean first = true;
        for (IChunkMetadata x : tmp)
          if (others.contains(x)) {
            chunkMdMightUpdate.add(x);
            others.remove(x);
            //            if (first) {
            //              System.out.print("\tOLD_Updated(");
            //              first = false;
            //            }
            //            debugChunkMd(x);
          }
        //        if (!first) System.out.print(")");
      }
    }

    for (IChunkMetadata another : others) {
      if (highFPR(checkingChunkMd, new ArrayList<>(Collections.singleton(another)))) {
        chunkMdMightUpdate.add(another);
        System.out.println(
            "\thighFPR single. " + checkingChunkMd.getStartTime() + " & " + another.getStartTime());
        if (prune) return new ReferenceOpenHashSet<>(Collections.singleton(another));
      }
    }
    others.removeAll(chunkMdMightUpdate);

    if (others.isEmpty()) {
      return chunkMdMightUpdate;
    }

    ObjectArrayList<BF> allBF =
        new ObjectArrayList<>(others.size() * checkingChunkMd.getStatistics().getBfNum());
    ObjectHeapPriorityQueue<BF> bfUsing =
        new ObjectHeapPriorityQueue<>(Comparator.comparingLong(x -> x.maxTime));
    ReferenceOpenHashSet<BF> bfUsingHashSet = new ReferenceOpenHashSet<>();
    //    System.out.print("\t/With new/:");
    for (IChunkMetadata another : others) {
      //      debugChunkMd(another);
      for (int i = 0; i < another.getStatistics().getBfNum(); i++) {
        BF bf = new BF(another, i);
        if (checkBFOverlapChunkMd(bf, checkingChunkMd)) allBF.add(bf);
      }
    }
    //    System.out.println();
    allBF.sort(Comparator.comparingLong(x -> x.minTime));
    ObjectHeapPriorityQueue<BF> bfToUse =
        new ObjectHeapPriorityQueue<>(allBF, Comparator.comparingLong(x -> x.minTime));

    List<IPageReader> pageReaderList = getPageReadersFromChunkMd(checkingChunkMd);
    LongArrayList timestampToCheck = new LongArrayList();
    for (IPageReader pageReader : pageReaderList) {
      Statistics pageStat = pageReader.getStatistics();
      while (!bfUsing.isEmpty() && bfUsing.first().maxTime < pageStat.getStartTime())
        bfUsingHashSet.remove(bfUsing.dequeue());
      if (bfUsing.isEmpty()
          && (bfToUse.isEmpty() || bfToUse.first().minTime > pageStat.getEndTime())) {
        continue; // current page no overlap.
      }
      IBatchDataIterator dataIt = pageReader.getAllSatisfiedPageData().getBatchDataIterator();
      while (dataIt.hasNext()) {
        while (!bfToUse.isEmpty() && bfToUse.first().minTime <= dataIt.currentTime()) {
          BF tmp = bfToUse.dequeue();
          if (!chunkMdMightUpdate.contains(tmp.chunkMd)) {
            bfUsing.enqueue(tmp);
            bfUsingHashSet.add(tmp);
          }
        }
        while (!bfUsing.isEmpty() && bfUsing.first().maxTime < dataIt.currentTime())
          bfUsingHashSet.remove(bfUsing.dequeue());
        long nextTime = Long.MAX_VALUE;
        if (!bfUsing.isEmpty()) nextTime = Math.min(nextTime, bfUsing.first().maxTime);
        if (!bfToUse.isEmpty()) nextTime = Math.min(nextTime, bfToUse.first().minTime - 1);
        while (dataIt.hasNext() && dataIt.currentTime() <= nextTime) {
          timestampToCheck.push(dataIt.currentTime());
          dataIt.next();
        }
        for (BF bf : bfUsingHashSet)
          if (!chunkMdMightUpdate.contains(bf.chunkMd)) {
            for (long timestamp : timestampToCheck)
              if (bf.bf.mightContain(timestamp)) {
                //                System.out.println(
                //                    "\t[DEBUG]\t\tBF POSITIVE!!\tt:"
                //                        + timestamp
                //                        + "\tanoBF:"
                //                        + bf.minTime
                //                        + "..."
                //                        + bf.maxTime
                //                        + "  bfIDs:"
                //                        + pageReaderList.indexOf(pageReader)
                //                        + "&"
                //                        + bf.bfId);
                chunkMdMightUpdate.add(bf.chunkMd);
                if (prune) return new ReferenceOpenHashSet<>(Collections.singleton(bf.chunkMd));
                break;
              }
          }
        bfUsingHashSet.removeIf(bf -> chunkMdMightUpdate.contains(bf.chunkMd));
        timestampToCheck.clear();
      }
    }
    //    if (!outside && !prune) // it is an inside check.
    {
      if (chunkUsedToUpdate.get(checkingChunkMd) == null)
        chunkUsedToUpdate.put(checkingChunkMd, chunkMdMightUpdate);
      else {
        chunkUsedToUpdate.get(checkingChunkMd).addAll(chunkMdMightUpdate);
      }
      chunkUsedToNoUpdate.putIfAbsent(checkingChunkMd, new ReferenceOpenHashSet<>(others.size()));
      for (IChunkMetadata x : others)
        if (!chunkMdMightUpdate.contains(x)) chunkUsedToNoUpdate.get(checkingChunkMd).add(x);
    }
    return chunkMdMightUpdate;
  }

  private void checkOneChunkMd() throws IOException {
    ReferenceOpenHashSet<IChunkMetadata> chunkMdMightUpdate;
    if (checkingChunkMd == null) {
      checkingIt = overlappedChunkMd.iterator(overlappedChunkMd.last());
    }
    checkingChunkMd = checkingIt.previous();
    if (checkingChunkMd == overlappedChunkMd.first()) {
      if (!chunkInsideUpdateSet.containsKey(checkingChunkMd)) {
        System.out.print("\t\t[DEBUG][CheckOneChunkMd]:\t" + "Found a NoUpdate ChunkMd!\t");
        debugChunkMd(checkingChunkMd);
        System.out.println();
        noUpdateChunkMd.push(checkingChunkMd);
      }
      return;
    }
    List<IChunkMetadata> insideToCheck = new ArrayList<>();
    List<IChunkMetadata> outsideToCheck = new ArrayList<>();
    for (IChunkMetadata another : chunkNeighbor.get(checkingChunkMd))
      if (!overlappedChunkMd.contains(another)) outsideToCheck.add(another);
      else if (chunkMdVersionSmaller(another, checkingChunkMd)) insideToCheck.add(another);
    chunkMdMightUpdate = checkMightUpdate(checkingChunkMd, insideToCheck, false, false);
    for (IChunkMetadata insideChunkMd : chunkMdMightUpdate)
      mergeUpdateSet(checkingChunkMd, insideChunkMd);
    if (chunkMdMightUpdate.isEmpty() && !chunkInsideUpdateSet.containsKey(checkingChunkMd)) {
      chunkMdMightUpdate = checkMightUpdate(checkingChunkMd, outsideToCheck, true, false);
      if (chunkMdMightUpdate != null && chunkMdMightUpdate.isEmpty()) {
        System.out.print("\t\t[DEBUG][CheckOneChunkMd]:\tFound a NoUpdate ChunkMd!\t");
        debugChunkMd(checkingChunkMd);
        System.out.println();
        noUpdateChunkMd.push(checkingChunkMd);
      }
    }
  }

  private void checkAllOverlapChunkMd() throws IOException {
    while (true) {
      checkOneChunkMd();
      if (checkingChunkMd == overlappedChunkMd.first()) break;
    }
    for (IChunkMetadata chunkMd : overlappedChunkMd) {
      if (chunkInsideUpdateSet.get(chunkMd) != null) {
        cachedChunkMetadata.add(chunkMd);

        //        System.out.print("\t\t[DEBUG] ChunkMd ");
        //        debugChunkMd(chunkMd);
        //        System.out.print("\tUpdate With (inside):");
        //        debugChunkMdSet(chunkInsideUpdateSet.get(chunkMd));
        //        System.out.println();
      }
    }
  }

  private void calcCntBlock()
      throws IOException { // called when currently the oldest chunk may be updated.

    System.out.println("\t[DEBUG][Oldest ChunkMd may update]");
    ReferenceOpenHashSet<IChunkMetadata> tmpSet =
        chunkInsideUpdateSet.get(overlappedChunkMd.first());
    //      long cntMinT = Long.MAX_VALUE,cntMaxT = Long.MIN_VALUE;
    //      for(IChunkMetadata chunkMd:tmpSet){
    //        cntMinT = Math.min(cntMinT,chunkMd.getStartTime());
    //        cntMaxT = Math.max(cntMaxT,chunkMd.getEndTime());
    //      }
    cntBlock.addAll(tmpSet);
    List<IChunkMetadata> listToCheck = new ArrayList<>();
    listToCheck.addAll(cntBlock);
    for (int i = 0; true; i++) {
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(getMaxTimeInChunkMdList(cntBlock));
      unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
          getMaxTimeInChunkMdList(cntBlock), false);
      List<IChunkMetadata> tmpList = new ArrayList<>();
      for (IChunkMetadata chunkMd : cachedChunkMetadata)
        if (!cntBlock.contains(chunkMd) && checkChunkMdOverlap(chunkMd, listToCheck)) {
          if (!checkMightUpdate(chunkMd, listToCheck, false, true).isEmpty()) tmpList.add(chunkMd);
        }
      if (i > 0)
        for (IChunkMetadata chunkMd : overlappedChunkMd)
          if (!cntBlock.contains(chunkMd)
              && !noUpdateChunkMd.contains(chunkMd)
              && checkChunkMdOverlap(chunkMd, listToCheck)) {
            if (!checkMightUpdate(chunkMd, listToCheck, false, true).isEmpty())
              tmpList.add(chunkMd);
          }
      if (tmpList.isEmpty()) break;
      listToCheck = tmpList;
      cntBlock.addAll(tmpList);
    }
    System.out.print("\t[DEBUG]\tblock we found:\t");
    debugChunkMdSet(new HashSet<>(cntBlock));
    System.out.println();
  }

  private void initLoadedChunkMd() {
    if (loadedChunkMd == null || loadedChunkMd.isEmpty())
      loadedChunkMd = new Reference2ReferenceOpenHashMap<>();
    else {
      long mxT = Long.MIN_VALUE;
      IChunkMetadata lastC = null;
      List<IPageReader> lastList = null;
      for (IChunkMetadata x : loadedChunkMd.keySet())
        if (x.getEndTime() > mxT) {
          mxT = x.getEndTime();
          lastC = x;
          lastList = loadedChunkMd.get(x);
        }
      loadedChunkMd = new Reference2ReferenceOpenHashMap<>();
      loadedChunkMd.put(lastC, lastList);
    }
  }

  private void initLoadedChunkMd(long minT) {
    if (loadedChunkMd == null || loadedChunkMd.isEmpty())
      loadedChunkMd = new Reference2ReferenceOpenHashMap<>();
    else {
      ReferenceList<IChunkMetadata> keyToDel = new ReferenceArrayList<>();
      for (IChunkMetadata x : loadedChunkMd.keySet())
        if (x.getEndTime() < minT) {
          keyToDel.add(x);
        }
      for (IChunkMetadata x : keyToDel) loadedChunkMd.remove(x);
    }
  }

  private void init() throws IOException {

    tryToUseBF = firstChunkMetadata.getStatistics().hasBf() && isChunkOverlapped();
    if (!tryToUseBF) return;

    overlappedChunkMd =
        new ObjectAVLTreeSet<>(
            (x, y) ->
                (x.getVersion() != y.getVersion()
                    ? Long.compare(x.getVersion(), y.getVersion())
                    : Long.compare(x.getOffsetOfChunkHeader(), y.getOffsetOfChunkHeader())));
    overlappedChunkMd.add(firstChunkMetadata);
    firstChunkMetadata = null;
    checkingChunkMd = null;
    System.out.println("\n\n\t[DEBUG][SeriesReaderForStat init]: start INIT.");
    getSomeOverlapChunkMd();

    initLoadedChunkMd(getMinTimeInChunkMdSet(overlappedChunkMd));

    chunkNeighbor = new Reference2ReferenceOpenHashMap<>(overlappedChunkMd.size());
    chunkInsideUpdateSet = new Reference2ReferenceOpenHashMap<>(overlappedChunkMd.size());
    checkOverlapDetail();

    noUpdateChunkMd = new ObjectArrayList<>();
    noUpdateBF = new ObjectArrayList<>();
    boundChunkMd = new ReferenceArrayList<>();
    checkAllOverlapChunkMd();
    cntBlock = new ArrayList<>();
    if (chunkInsideUpdateSet.get(overlappedChunkMd.first()) != null) {
      calcCntBlock();
    }
    for (IChunkMetadata x : overlappedChunkMd)
      if (!noUpdateChunkMd.contains(x)) cachedChunkMetadata.add(x);
    cachedChunkMetadata.removeAll(cntBlock);
    for (IChunkMetadata x : CollectionUtils.union(noUpdateChunkMd, cntBlock)) {
      chunkUsedToNoUpdate.remove(x);
      chunkUsedToUpdate.remove(x);
    }
    //    tryToUseBF = false;
  }

  public boolean hasNoUpdateChunkMd() {
    return !noUpdateChunkMd.isEmpty();
  }

  public boolean canUseNoUpdateChunkStat() {
    if (!hasNoUpdateChunkMd()) return false;
    IChunkMetadata chunkMd = noUpdateChunkMd.top();
    return !chunkMd.isModified()
        && (timeFilter == null
            || timeFilter.containStartEndTime(chunkMd.getStartTime(), chunkMd.getEndTime()));
  }

  public IChunkMetadata popNoUpdateChunkMd() {
    return noUpdateChunkMd.pop();
  }

  public void giveUpNoUpdateChunkMd() throws IOException {
    System.out.println("\t\t\tgiveUpNoUpdateChunkMd.");
    readingOldPages = true;
    firstChunkMetadata = noUpdateChunkMd.pop();
    getBoundChunkMd(firstChunkMetadata.getEndTime());
  }

  public void readyToUseNoUpdateChunkMd() {
    releaseBoundChunkMd();
    if (!hasNoUpdateChunkMd()) {
      tryToUseBF = usingBF = false;
      return;
    }
  }

  @Override
  public boolean hasNextPage() throws IOException {
    if (firstChunkMetadata != null
        && !tryToUseBF
        && firstChunkMetadata.getStartTime() > lastInitTime) {
      init();
      if (!tryToUseBF) {
        boolean has = super.hasNextPage();
        return has;
      }
      if (!cntBlock.isEmpty()) {
        readingOldPages = true;
        getBoundChunkMd(getMaxTimeInChunkMdList(cntBlock));
        cachedChunkMetadata.addAll(cntBlock);
        cntBlock.clear();
        return super.hasNextPage();
      } else readingOldPages = false;

      return true;
    }
    if (readingOldPages) {
      //      System.out.print("\t[DEBUG][SeriesReaderForStat hasNextPage]:" +
      //      " readingOldPages");
      boolean hasOldPages = super.hasNextPage();
      //      System.out.println(":" + hasOldPages);
      readingOldPages = hasOldPages;
      return true;
    }
    if (!tryToUseBF) return super.hasNextPage();
    return true;
  }

  @Override
  BatchData nextPage() throws IOException {
    if (!tryToUseBF) return super.nextPage();
    if (readingOldPages) return super.nextPage();
    return null;
  }
}
