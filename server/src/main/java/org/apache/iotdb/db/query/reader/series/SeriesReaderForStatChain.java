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
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

import com.google.common.hash.BloomFilter;
import it.unimi.dsi.fastutil.objects.*;

import java.io.IOException;
import java.util.*;

public class SeriesReaderForStatChain extends SeriesReader {
  private ObjectHeapPriorityQueue<ObjectIntMutablePair<IChunkMetadata>> chunkCntPageStat;
  // use changed();
  private ObjectHeapPriorityQueue<IChunkMetadata> chunkToCheckBF;

  public boolean tryToUseBF = false;
  public boolean usingBF = false;
  public boolean readingOldPages = false;
  public long lastInitTime = Long.MIN_VALUE;

  public SeriesReaderForStatChain(
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

  private boolean firstPageOverlappedOtherChunk() throws IOException {
    if (firstPageReader == null) {
      return false;
    }
    long endpointTime = orderUtils.getOverlapCheckTime(firstPageReader.getStatistics());
    unpackAllOverlappedTsFilesToTimeSeriesMetadata(endpointTime);
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(endpointTime, false);
    return (!cachedChunkMetadata.isEmpty()
        && orderUtils.isOverlapped(
            firstPageReader.getStatistics(), cachedChunkMetadata.first().getStatistics()));
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

  private void initChunkCntPageStat(long l, long r) {
    for (IChunkMetadata chunkMd : cachedChunkMetadata)
      if (checkPointInRange(l, chunkMd.getStartTime(), chunkMd.getEndTime())) {
        DoubleStatistics stat = (DoubleStatistics) chunkMd.getStatistics();
        for (int i = 0; i < stat.getBfNum(); i++)
          if (checkPointInRange(l, stat.getBFMinTime(i), stat.getBFMaxTime(i))) {
            chunkCntPageStat.enqueue(new ObjectIntMutablePair<>(chunkMd, i));
            break;
          }
      } else if (l < chunkMd.getStartTime()
          && checkPointInRange(r, chunkMd.getStartTime(), chunkMd.getEndTime())) {
        chunkToCheckBF.enqueue(chunkMd);
      }
  }

  Object2ObjectOpenHashMap<IChunkMetadata, List<IPageReader>> loadedChunkMd;
  ObjectAVLTreeSet<IChunkMetadata> overlappedChunkMd;
  IChunkMetadata boundChunkMd = null;
  Stack<IChunkMetadata> noUpdateChunkMd;
  List<IChunkMetadata> chain;
  int chainLength;

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

  private void addOverlappedChunkMd(IChunkMetadata chunkMd) {
    overlappedChunkMd.add(chunkMd);
  }

  // no point overlap with old chunks and new chunks
  private boolean canDivideChunksByPoint(long lastEndTime) {
    return (lastEndTime < cachedChunkMetadata.first().getStartTime());
  }

  private boolean canDivideChunksAsLinear(IChunkMetadata chunkMd) {
    int oldOverlapCnt = 0;
    for (IChunkMetadata another : overlappedChunkMd)
      oldOverlapCnt += checkChunkMdOverlap(chunkMd, another) ? 1 : 0;
    if (oldOverlapCnt > 1) return false;
    //    if (oldOverlapCnt == 0)
    //      System.out.println("\t[ERROR][SeriesReaderForStat canDivideLinear]:noOld");

    int newOverlapCnt = 0;
    for (IChunkMetadata another : cachedChunkMetadata)
      newOverlapCnt += checkChunkMdOverlap(chunkMd, another) ? 1 : 0;
    if (newOverlapCnt > 1) return false;
    //    if (newOverlapCnt == 0)
    //      System.out.println("\t[ERROR][SeriesReaderForStat canDivideLinear]:noNew");

    //    System.out.println(
    //        "\t[DEBUG][SeriesReaderForStat canDivideChunksAsLinear]:"
    //            + oldOverlapCnt
    //            + ","
    //            + newOverlapCnt);
    return true;
  }

  private int tryToDivideChunks(
      IChunkMetadata chunkMd, IChunkMetadata preChunkMd, IChunkMetadata nextChunkMd)
      throws IOException {
    List<IPageReader> pageReaderList = getPageReadersFromChunkMd(chunkMd);
    boolean noOldUpdate = true, noNewUpdate = true;
    int preChunkBfId = 0, nextChunkBfId = -1;
    Statistics preChunkStat = preChunkMd.getStatistics();
    Statistics nextChunkStat = nextChunkMd.getStatistics();
    for (IPageReader pageReader : pageReaderList) {
      if (pageReader.getStatistics().getStartTime() > preChunkMd.getEndTime()
          && pageReader.getStatistics().getEndTime() < nextChunkMd.getStartTime()) continue;
      for (IBatchDataIterator it = pageReader.getAllSatisfiedPageData().getBatchDataIterator();
          it.hasNext();
          it.next()) {
        while (preChunkBfId < preChunkStat.getBfNum()
            && it.currentTime() > preChunkStat.getBfMaxTime(preChunkBfId)) preChunkBfId++;
        while (nextChunkBfId + 1 < nextChunkStat.getBfNum()
            && it.currentTime() >= preChunkStat.getBfMinTime(nextChunkBfId + 1)) nextChunkBfId++;
        if (preChunkBfId < preChunkStat.getBfNum())
          noOldUpdate &= !(preChunkStat.getBf(preChunkBfId)).mightContain(it.currentTime());

        if (nextChunkBfId >= 0)
          noNewUpdate &= !(nextChunkStat.getBf(nextChunkBfId)).mightContain(it.currentTime());
      }
      if (!noOldUpdate && !noNewUpdate) return 3;
    }
    return (noOldUpdate ? 0 : 1) + (noNewUpdate ? 0 : 2);
  }

  public boolean checkChunkMdOnChainWithAnother(IChunkMetadata chunkMd, IChunkMetadata another)
      throws IOException {
    List<IPageReader> pageReaderList = getPageReadersFromChunkMd(chunkMd);
    Statistics nextChunkStat = another.getStatistics();
    int nextChunkBfId = 0;
    //    boolean first = true;
    for (IPageReader pageReader : pageReaderList)
      if (pageReader.getStatistics().getEndTime() >= another.getStartTime()
          && pageReader.getStatistics().getStartTime() <= another.getEndTime()) {
        for (IBatchDataIterator it = pageReader.getAllSatisfiedPageData().getBatchDataIterator();
            it.hasNext();
            it.next()) {
          //          if (first) {
          //            System.out.println("\t\t\t\t\tdebug it firstT:" + it.currentTime());
          //            first = false;
          //          }
          while (nextChunkBfId < nextChunkStat.getBfNum()
              && nextChunkStat.getBfMaxTime(nextChunkBfId) < it.currentTime()) nextChunkBfId++;
          if (nextChunkBfId >= nextChunkStat.getBfNum()) return true;
          if (nextChunkStat.getBf(nextChunkBfId).mightContain(it.currentTime())) return false;
        }
      }
    return true;
  }

  private void getSomeOverlapChunkMd() throws IOException {
    long cntEndTime = firstChunkMetadata.getEndTime();
    unpackAllOverlappedTsFilesToTimeSeriesMetadata(cntEndTime);
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(cntEndTime, false);
    boolean NonOverlapEnd = false, DivideSuccess = false, recentlyDivideFailed = false;
    chainLength = cachedChunkMetadata.size() == 1 ? 1 : 0;
    while (!cachedChunkMetadata.isEmpty()) {
      if (cachedChunkMetadata.first().getStartTime() > cntEndTime) {
        NonOverlapEnd = true;
        break;
      } else {
        IChunkMetadata chunkMd = cachedChunkMetadata.first();
        cachedChunkMetadata.remove(chunkMd);
        //        System.out.print("\t[DEBUG][SeriesReaderForQuantile getSomeOverlapChunkMd]" +
        //            " chunkMd:");
        //        debugChunkMd(chunkMd);
        //        System.out.println();
        if (chunkMd.getEndTime() > cntEndTime) {
          long lastEndTime = cntEndTime;
          cntEndTime = chunkMd.getEndTime();
          unpackAllOverlappedTsFilesToTimeSeriesMetadata(cntEndTime);
          unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(cntEndTime, false);
          if (cachedChunkMetadata.isEmpty()
              || cachedChunkMetadata.first().getStartTime() > cntEndTime) {
            NonOverlapEnd = true;
            if (canDivideChunksAsLinear(chunkMd)) {
              addOverlappedChunkMd(chunkMd);
              chainLength++;
              DivideSuccess = true;
            } else {
              addOverlappedChunkMd(chunkMd);
              tryToUseBF = false;
              return;
            }
            break;
          }
          if (!canDivideChunksByPoint(lastEndTime) || !canDivideChunksAsLinear(chunkMd)) {
            //            System.out.println(
            //                "\t[DEBUG][SeriesReaderForQuantile getSomeOverlapChunkMd]"
            //                    + "chainLength<-0 : chunkMd can't Divide");
            chainLength = 0;
            addOverlappedChunkMd(chunkMd);
            if (overlappedChunkMd.size() >= 30) {
              tryToUseBF = false;
              return;
            }
          } else {
            if (chainLength >= 10 && !recentlyDivideFailed) {
              int divide_result =
                  tryToDivideChunks(chunkMd, overlappedChunkMd.last(), cachedChunkMetadata.first());
              //              System.out.println(
              //                  "\t[DEBUG][SeriesReaderForQuantile getSomeOverlapChunkMd]"
              //                      + "\ttry to divide as linear."
              //                      + "\tchunkMdEndT:"
              //                      + chunkMd.getEndTime()
              //                      + "\tresult:"
              //                      + divide_result);
              if (divide_result == 3 || divide_result == 1) {
                recentlyDivideFailed = true;
                addOverlappedChunkMd(chunkMd);
                chainLength++;
              } else {
                DivideSuccess = true;
                //                if (divide_result == 2) {
                boundChunkMd = chunkMd;
                //                } else {
                //                  addOverlappedChunkMd(chunkMd);
                //                  chainLength++;
                //                }
                break;
              }
            } else {
              addOverlappedChunkMd(chunkMd);
              chainLength++;
              recentlyDivideFailed = false;
            }
          }
        } else {
          //          System.out.println(
          //              "\t[DEBUG][SeriesReaderForQuantile getSomeOverlapChunkMd]"
          //                  + "chainLength<-0 : endTime don't increase");
          addOverlappedChunkMd(chunkMd);
          chainLength = 0;
        }
      }
    }
    //    System.out.println(
    //        "\t[DEBUG][SeriesReaderForQuantile getOverlapChunkMd]"
    //            + "\tChunkMd_Num:"
    //            + overlappedChunkMd.size()
    //            + "\tchainLen:"
    //            + chainLength
    //            + " all skip ^ ^");
    //    if (NonOverlapEnd)
    //      System.out.println(
    //          "\t[DEBUG][SeriesReaderForQuantile getOverlapChunkMd]"
    //              + "\tNonOverlapEnd. don't need to divide");
    //    if (DivideSuccess)
    //      System.out.println(
    //          "\t[DEBUG][SeriesReaderForQuantile getOverlapChunkMd]"
    //              + "\tDivideSuccess."
    //              + "\tchainLength:"
    //              + chainLength);
    //    for (IChunkMetadata chunkMd : overlappedChunkMd)
    //      System.out.println(
    //          "\t\tOverlappedChunkMd: " + chunkMd.getStartTime() + "..." + chunkMd.getEndTime());
    if (!DivideSuccess) tryToUseBF = false;
  }

  private void initLoadedChunkMd() {

    if (loadedChunkMd == null || loadedChunkMd.isEmpty())
      loadedChunkMd = new Object2ObjectOpenHashMap<>();
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
      loadedChunkMd = new Object2ObjectOpenHashMap<>();
      loadedChunkMd.put(lastC, lastList);
    }
  }

  private void init() throws IOException {
    noUpdateChunkMd = new Stack<>();
    initLoadedChunkMd();
    overlappedChunkMd =
        new ObjectAVLTreeSet<>(Comparator.comparingLong(IChunkMetadata::getEndTime));
    overlappedChunkMd.add(firstChunkMetadata);
    boundChunkMd = null;

    //    System.out.println(
    //        "\t\tfirstChunkMetadata: "
    //            + firstChunkMetadata.getStartTime()
    //            + "..."
    //            + firstChunkMetadata.getEndTime());
    //    System.out.println(
    //        "\t[DEBUG][SeriesReaderForStat init]: " + "cachedChunkMdNum:" +
    // cachedChunkMetadata.size());
    //    for (IChunkMetadata chunkMd : cachedChunkMetadata)
    //      System.out.println(
    //          "\t\tcachedChunkMetadata: " + chunkMd.getStartTime() + "..." +
    // chunkMd.getEndTime());
    //    System.out.println(
    //        "\t[DEBUG][SeriesReaderForStat init]: "
    //            + "cachedTSMdNum:"
    //            + (unSeqTimeSeriesMetadata.size() + seqTimeSeriesMetadata.size()));
    tryToUseBF = firstChunkMetadata.getStatistics().hasBf() && isChunkOverlapped();
    if (!tryToUseBF) return;
    //    System.out.println("\t[DEBUG][SeriesReaderForStat init]: start INIT.");
    getSomeOverlapChunkMd();
    firstChunkMetadata = null;
    if (!tryToUseBF) {
      lastInitTime = overlappedChunkMd.last().getStartTime();
      cachedChunkMetadata.addAll(overlappedChunkMd);
      firstChunkMetadata = cachedChunkMetadata.first();
      cachedChunkMetadata.remove(firstChunkMetadata);
    } else {
      chain = new ArrayList<>(chainLength);
      for (int i = 0; i < chainLength; i++) {
        IChunkMetadata chunkMd = overlappedChunkMd.last();
        chain.add(chunkMd);
        overlappedChunkMd.remove(chunkMd);
      }
      Collections.reverse(chain);
      //      System.out.print("\t[DEBUG][SeriesReaderForStat init]: MdChain: ");
      //      for (IChunkMetadata chunkMd : chain) {
      //        debugChunkMd(chunkMd);
      //      }
      //      System.out.println();
    }
  }

  private void giveUpChain(int pos) {
    //    System.out.println("\t\t\tdebug\tgiveUpChain\tpos:" + pos);
    cachedChunkMetadata.addAll(chain.subList(pos, chain.size()));
    firstChunkMetadata = cachedChunkMetadata.first();
    cachedChunkMetadata.remove(firstChunkMetadata);
    while (chain.size() > pos) chain.remove(pos);
    readingOldPages = true;
  }

  public void readyToUseChain() throws IOException {
    //    System.out.print("\t[DEBUG][SeriesReaderForStat readyToUseChain]: Ready to Use Chain: ");
    //    for (IChunkMetadata chunkMd : chain) {
    //      debugChunkMd(chunkMd);
    //    }
    //    System.out.println();
    if (!noUpdateChunkMd.isEmpty()) {
      //      System.out.println("\t\t\tdebug useChain: already have noUpdateChunkMd.");
      return;
    }

    if (chain.isEmpty()) {
      if (boundChunkMd != null) {
        cachedChunkMetadata.add(boundChunkMd);
        boundChunkMd = null;
      }
      tryToUseBF = usingBF = false;
      return;
    }
    usingBF = true;
    int pos = chain.size() - 2;
    if (pos >= 1) {
      int result = tryToDivideChunks(chain.get(pos), chain.get(pos - 1), chain.get(pos + 1));
      if (result <= 1) noUpdateChunkMd.add(chain.remove(pos + 1));
      if (result == 0) noUpdateChunkMd.add(chain.remove(pos));
      if (result % 2 == 1) {
        pos--;
        while (pos >= 1 && !checkChunkMdOnChainWithAnother(chain.get(pos), chain.get(pos - 1)))
          pos--;
        giveUpChain(pos);
      }
      if (result == 2) giveUpChain(pos);
    } else if (pos == 0) {
      int idToUnpack =
          (loadedChunkMd.containsKey(chain.get(1)) || !loadedChunkMd.containsKey(chain.get(0)))
              ? 1
              : 0;
      if (!checkChunkMdOnChainWithAnother(chain.get(idToUnpack), chain.get(idToUnpack ^ 1)))
        giveUpChain(0);
      else {
        noUpdateChunkMd.add(chain.remove(1));
        noUpdateChunkMd.add(chain.remove(0));
      }
    } else {
      noUpdateChunkMd.add(chain.remove(0));
    }
    //    if (readingOldPages) {
    //      System.out.print("\t\t\tdebug useChain:fail\tremaining:");
    //      for (IChunkMetadata chunkMd : chain) {
    //        debugChunkMd(chunkMd);
    //      }
    //      System.out.println();
    //    } else {
    //      System.out.print("\t\t\tdebug useChain:success\tremaining:");
    //      for (IChunkMetadata chunkMd : chain) {
    //        debugChunkMd(chunkMd);
    //      }
    //      System.out.println();
    //    }
  }

  @Override
  public boolean hasNextPage() throws IOException {
    if (firstChunkMetadata != null
        && !tryToUseBF
        && firstChunkMetadata.getStartTime() > lastInitTime) {
      init();
      if (!tryToUseBF) {

        //        System.out.println(
        //            "\t[DEBUG][SeriesReaderForStat hasNextPage]: don't use BF."
        //                + "\tfirstT:"
        //                + firstChunkMetadata.getStatistics().getStartTime()
        //                + "\tcachedFT:"
        //                + cachedChunkMetadata.peek().getStartTime()
        //                + "\tcachedChunkMdSize:"
        //                + cachedChunkMetadata.size());
        //        for (IChunkMetadata chunkMd : cachedChunkMetadata)
        //          System.out.println(
        //              "\t\t\tdebug\t"
        //                  + chunkMd.getStartTime()
        //                  + "..."
        //                  + chunkMd.getEndTime()
        //                  + " seq:"
        //                  + chunkMd.isSeq());
        boolean has = super.hasNextPage();
        //        System.out.println("\t\t\t！！！ super.hasNextPage:\t" + has);
        return has;
      }
      cachedChunkMetadata.addAll(overlappedChunkMd);
      if (!overlappedChunkMd.isEmpty()) {
        IChunkMetadata lastMd = overlappedChunkMd.last();
        for (int i = 0; i < chain.size(); i++)
          if (!checkChunkMdOnChainWithAnother(chain.get(i), lastMd)) {
            cachedChunkMetadata.add(chain.get(i));
            lastMd = chain.get(i);
            chainLength--;
            if (chainLength < chain.size() / 2) {
              tryToUseBF = false;
              break;
            }
          } else break;
        for (int i = chain.size() - chainLength; i > 0; i--) chain.remove(0);
        if (!tryToUseBF) {
          cachedChunkMetadata.addAll(chain);
        }
        //        System.out.println("\t[DEBUG][divide Chain]: startT:" +
        //            chain.get(0).getStartTime());
        readingOldPages = true;
        firstChunkMetadata = cachedChunkMetadata.first();
        cachedChunkMetadata.remove(firstChunkMetadata);
        return super.hasNextPage();
      } else readingOldPages = false;
      return true;
    }
    if (readingOldPages) {
      //      System.out.print("\t[DEBUG][SeriesReaderForStat hasNextPage]:"+
      //      " readingOldPages");
      boolean hasOldPages = super.hasNextPage();
      //      System.out.println(":" + hasOldPages);
      readingOldPages = hasOldPages;
      return true;
    }
    if (!tryToUseBF) return super.hasNextPage();
    return true;
  }

  public boolean hasNoUpdateChunkMd() {
    return !noUpdateChunkMd.isEmpty();
  }

  public boolean canUseNoUpdateChunkStat() {
    if (!hasNoUpdateChunkMd()) return false;
    IChunkMetadata chunkMd = noUpdateChunkMd.peek();
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
  }

  @Override
  BatchData nextPage() throws IOException {
    if (!tryToUseBF) return super.nextPage();
    if (readingOldPages) return super.nextPage();
    return null;
  }
}
