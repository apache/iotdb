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

package org.apache.iotdb.db.queryengine.execution.load.nodesplit;

import org.apache.iotdb.db.queryengine.execution.load.AlignedChunkData;
import org.apache.iotdb.db.queryengine.execution.load.ChunkData;
import org.apache.iotdb.db.queryengine.execution.load.NonAlignedChunkData;
import org.apache.iotdb.db.queryengine.execution.load.TsFileData;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Chunk;

import net.ricecode.similarity.LevenshteinDistanceStrategy;
import net.ricecode.similarity.SimilarityStrategy;
import net.ricecode.similarity.StringSimilarityService;
import net.ricecode.similarity.StringSimilarityServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ClusteringMeasurementSplitter implements PieceNodeSplitter {

  private static final Logger logger = LoggerFactory.getLogger(ClusteringMeasurementSplitter.class);
  private double groupFactor;
  private int maxIteration;
  private int dataSampleLength = 32;
  private double stdErrThreshold = 1;
  private long splitStartTime;
  private long splitTimeBudget = 5;
  private static final int MAX_CLUSTER_NUM = 500;

  public ClusteringMeasurementSplitter(double groupFactor, int maxIteration) {
    this.groupFactor = groupFactor;
    this.maxIteration = maxIteration;
  }

  @Override
  public List<LoadTsFilePieceNode> split(LoadTsFilePieceNode pieceNode) {
    splitStartTime = System.currentTimeMillis();
    if (pieceNode.isHasModification()) {
      // the order of modifications should be preserved, so with modifications clustering cannot be
      // used
      return new OrderedMeasurementSplitter().split(pieceNode);
    }

    // split by measurement first
    Map<String, LoadTsFilePieceNode> measurementPieceNodeMap =
        new HashMap<>(pieceNode.getAllTsFileData().size());
    for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
      ChunkData chunkData = (ChunkData) tsFileData;
      String currMeasurement = chunkData.firstMeasurement();
      LoadTsFilePieceNode pieceNodeSplit =
          measurementPieceNodeMap.computeIfAbsent(
              currMeasurement,
              m -> new LoadTsFilePieceNode(pieceNode.getPlanNodeId(), pieceNode.getTsFile()));
      pieceNodeSplit.addTsFileData(chunkData);
    }
    // use clustering to merge similar measurements
    List<LoadTsFilePieceNode> loadTsFilePieceNodes = clusterPieceNode(measurementPieceNodeMap);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Split distribution before refinement: {}",
          loadTsFilePieceNodes.stream()
              .map(l -> l.getAllTsFileData().size())
              .collect(Collectors.toList()));
      refineClusters(loadTsFilePieceNodes);
      logger.debug(
          "Split distribution after refinement: {}",
          loadTsFilePieceNodes.stream()
              .map(l -> l.getAllTsFileData().size())
              .collect(Collectors.toList()));
    }
    return loadTsFilePieceNodes;
  }

  private void refineClusters(List<LoadTsFilePieceNode> pieceNodes) {
    double average =
        pieceNodes.stream().mapToLong(LoadTsFilePieceNode::getDataSize).average().orElse(0.0);
    double finalAverage1 = average;
    double stderr =
        Math.sqrt(
            pieceNodes.stream()
                    .mapToLong(LoadTsFilePieceNode::getDataSize)
                    .mapToDouble(s -> (s - finalAverage1) * (s - finalAverage1))
                    .sum()
                / pieceNodes.size());
    logger.debug(
        "{} splits before refinement, average/stderr: {}/{}", pieceNodes.size(), average, stderr);

    while (stderr > average * stdErrThreshold
        && System.currentTimeMillis() - splitStartTime < splitTimeBudget) {
      pieceNodes.sort(Comparator.comparingLong(LoadTsFilePieceNode::getDataSize));
      LoadTsFilePieceNode smallestPiece = pieceNodes.get(0);
      LoadTsFilePieceNode largestPiece = pieceNodes.get(pieceNodes.size() - 1);
      double smallDiff = average - smallestPiece.getDataSize();
      double largeDiff = largestPiece.getDataSize() - average;
      if (smallDiff > largeDiff) {
        mergeSmallPiece(pieceNodes);
      } else {
        if (!splitLargePiece(pieceNodes)) {
          // the largest node may not be splittable (only one series), merge the smallest instead
          mergeSmallPiece(pieceNodes);
        }
      }

      average =
          pieceNodes.stream().mapToLong(LoadTsFilePieceNode::getDataSize).average().orElse(0.0);
      double finalAverage = average;
      stderr =
          Math.sqrt(
              pieceNodes.stream()
                      .mapToLong(LoadTsFilePieceNode::getDataSize)
                      .mapToDouble(s -> (s - finalAverage) * (s - finalAverage))
                      .sum()
                  / pieceNodes.size());
      logger.debug("{} splits, average/stderr: {}/{}", pieceNodes.size(), average, stderr);
    }
    logger.debug(
        "{} splits after refinement, average/stderr: {}/{}", pieceNodes.size(), average, stderr);
  }

  private void mergeSmallPiece(List<LoadTsFilePieceNode> pieceNodes) {
    LoadTsFilePieceNode pieceA = pieceNodes.remove(0);
    LoadTsFilePieceNode pieceB = pieceNodes.remove(0);
    LoadTsFilePieceNode newPiece =
        new LoadTsFilePieceNode(pieceA.getPlanNodeId(), pieceA.getTsFile());
    pieceA.getAllTsFileData().forEach(newPiece::addTsFileData);
    pieceB.getAllTsFileData().forEach(newPiece::addTsFileData);
    pieceNodes.add(newPiece);
    pieceNodes.stream().mapToLong(LoadTsFilePieceNode::getDataSize).average().orElse(0.0);
    pieceNodes.stream().mapToLong(LoadTsFilePieceNode::getDataSize).sum();
  }

  private boolean splitLargePiece(List<LoadTsFilePieceNode> pieceNodes) {
    LoadTsFilePieceNode oldPiece = pieceNodes.remove(pieceNodes.size() - 1);
    LoadTsFilePieceNode newNodeA =
        new LoadTsFilePieceNode(oldPiece.getPlanNodeId(), oldPiece.getTsFile());
    LoadTsFilePieceNode newNodeB =
        new LoadTsFilePieceNode(oldPiece.getPlanNodeId(), oldPiece.getTsFile());
    long sizeTarget = oldPiece.getDataSize() / 2;

    // cannot break a series into two pieces since the chunk order must be preserved
    String currMeasurement = null;
    List<TsFileData> allTsFileData = oldPiece.getAllTsFileData();
    int i = 0;
    for (; i < allTsFileData.size(); i++) {
      TsFileData tsFileData = allTsFileData.get(i);
      if (tsFileData.isModification()) {
        // modifications follows previous chunk data
        newNodeA.addTsFileData(tsFileData);
      } else {
        ChunkData chunkData = (ChunkData) tsFileData;
        if (currMeasurement == null || currMeasurement.equals(chunkData.firstMeasurement()) ||
            sizeTarget > 0) {
          // the first chunk or chunk of the same series, add it to A
          // or the chunk of the next series and splitA is not
          currMeasurement = chunkData.firstMeasurement();
          newNodeA.addTsFileData(tsFileData);
          sizeTarget -= chunkData.getDataSize();
        } else if (sizeTarget < 0) {
          // a new series but splitA is full, break to fill splitB
          break;
        }
      }
    }
    // add remaining series to B
    for (; i < allTsFileData.size(); i++) {
      TsFileData tsFileData = allTsFileData.get(i);
      newNodeB.addTsFileData(tsFileData);
    }

    pieceNodes.add(newNodeA);
    if (!newNodeB.getAllTsFileData().isEmpty()) {
      pieceNodes.add(newNodeB);
      return true;
    }
    return false;
  }

  private List<LoadTsFilePieceNode> clusterPieceNode(
      Map<String, LoadTsFilePieceNode> measurementPieceNodeMap) {
    // convert to feature vector
    Map<String, SeriesFeatureVector> measurementVectorMap =
        new HashMap<>(measurementPieceNodeMap.size());
    for (Entry<String, LoadTsFilePieceNode> entry : measurementPieceNodeMap.entrySet()) {
      measurementVectorMap.put(entry.getKey(), convertToFeature(entry.getValue()));
    }
    // normalize
    normalize(measurementVectorMap.values());
    Map<String, double[]> doubleVectors = new HashMap<>(measurementPieceNodeMap.size());
    for (Entry<String, SeriesFeatureVector> e : measurementVectorMap.entrySet()) {
      doubleVectors.put(e.getKey(), e.getValue().numericVector);
    }
    // clustering
    int numCluster = Math.min((int) (doubleVectors.size() / groupFactor), MAX_CLUSTER_NUM);
    if (numCluster < 1) {
      numCluster = 1;
    }
    VectorDistance distance = new EuclideanDistance();
    Clustering clustering = new KMeans(numCluster, maxIteration);
    List<List<String>> clusterResult = clustering.cluster(doubleVectors, distance);
    // collect result
    List<LoadTsFilePieceNode> clusteredNodes = new ArrayList<>();
    for (List<String> cluster : clusterResult) {
      if (cluster.isEmpty()) {
        continue;
      }
      LoadTsFilePieceNode pieceNode = measurementPieceNodeMap.get(cluster.get(0));
      LoadTsFilePieceNode clusterNode =
          new LoadTsFilePieceNode(pieceNode.getPlanNodeId(), pieceNode.getTsFile());
      for (String measurementId : cluster) {
        pieceNode = measurementPieceNodeMap.get(measurementId);
        for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
          clusterNode.addTsFileData(tsFileData);
        }
      }
      clusteredNodes.add(clusterNode);
    }

    return clusteredNodes;
  }

  private void normalize(Collection<SeriesFeatureVector> vectors) {
    String firstMeasurementId = null;
    int minDataSize = Integer.MAX_VALUE;
    int maxDataSize = Integer.MIN_VALUE;
    int minDataType = Integer.MAX_VALUE;
    int maxDataType = Integer.MIN_VALUE;
    int minCompressionType = Integer.MAX_VALUE;
    int maxCompressionType = Integer.MIN_VALUE;
    int minEncodingType = Integer.MAX_VALUE;
    int maxEncodingType = Integer.MIN_VALUE;
    int minNumOfPages = Integer.MAX_VALUE;
    int maxNumOfPages = Integer.MIN_VALUE;
    String firstDataSample = null;
    for (SeriesFeatureVector vector : vectors) {
      if (firstMeasurementId == null) {
        firstMeasurementId = vector.measurementId;
        firstDataSample = vector.dataSample;
      }
      minDataSize = Math.min(minDataSize, vector.dataSize);
      maxDataSize = Math.max(maxDataSize, vector.dataSize);
      minDataType = Math.min(minDataType, vector.dataType.ordinal());
      maxDataType = Math.max(maxDataType, vector.dataType.ordinal());
      minCompressionType = Math.min(minCompressionType, vector.compressionType.ordinal());
      maxCompressionType = Math.max(maxCompressionType, vector.compressionType.ordinal());
      minEncodingType = Math.min(minEncodingType, vector.encodingType.ordinal());
      maxEncodingType = Math.max(maxEncodingType, vector.encodingType.ordinal());
      minNumOfPages = Math.min(minNumOfPages, vector.numOfPages);
      maxNumOfPages = Math.max(maxNumOfPages, vector.numOfPages);
    }

    SimilarityStrategy strategy = new LevenshteinDistanceStrategy();
    StringSimilarityService service = new StringSimilarityServiceImpl(strategy);
    int finalMinDataSize = minDataSize;
    String finalFirstMeasurementId = firstMeasurementId;
    int finalMaxDataSize = maxDataSize;
    int finalMinDataType = minDataType;
    int finalMaxDataType = maxDataType;
    int finalMinCompressionType = minCompressionType;
    int finalMaxCompressionType = maxCompressionType;
    int finalMinEncodingType = minEncodingType;
    int finalMaxEncodingType = maxEncodingType;
    int finalMinNumOfPages = minNumOfPages;
    int finalMaxNumOfPages = maxNumOfPages;
    String finalFirstDataSample = firstDataSample;
    vectors.stream()
        .parallel()
        .forEach(
            vector -> {
              vector.numericVector[0] =
                  service.score(finalFirstMeasurementId, vector.measurementId);
              vector.numericVector[1] =
                  (vector.dataSize - finalMinDataSize)
                      * 1.0
                      / (finalMaxDataSize - finalMinDataSize);
              vector.numericVector[2] =
                  (vector.dataType.ordinal() - finalMinDataType)
                      * 1.0
                      / (finalMaxDataType - finalMinDataType);
              vector.numericVector[3] =
                  (vector.compressionType.ordinal() - finalMinCompressionType)
                      * 1.0
                      / (finalMaxCompressionType - finalMinCompressionType);
              vector.numericVector[4] =
                  (vector.encodingType.ordinal() - finalMinEncodingType)
                      * 1.0
                      / (finalMaxEncodingType - finalMinEncodingType);
              vector.numericVector[5] =
                  (vector.numOfPages - finalMinNumOfPages)
                      * 1.0
                      / (finalMaxNumOfPages - finalMinNumOfPages);
              vector.numericVector[6] = service.score(finalFirstDataSample, vector.dataSample);
              double[] numericVector = vector.numericVector;
              for (int i = 0; i < numericVector.length; i++) {
                if (Double.isNaN(numericVector[i])) {
                  numericVector[i] = 0.0;
                } else if (Double.isInfinite(numericVector[i])) {
                  numericVector[i] = 1.0;
                }
              }
            });
  }

  private SeriesFeatureVector convertToFeature(LoadTsFilePieceNode pieceNode) {
    List<TsFileData> allTsFileData = pieceNode.getAllTsFileData();
    SeriesFeatureVector vector = null;
    for (TsFileData tsFileData : allTsFileData) {
      ChunkData chunkData = (ChunkData) tsFileData;
      if (vector == null) {
        vector = SeriesFeatureVector.fromChunkData(chunkData, dataSampleLength);
      } else {
        vector.mergeChunkData(chunkData);
      }
    }
    return vector;
  }

  public static class SeriesFeatureVector {

    private String measurementId;
    private int dataSize;
    private TSDataType dataType;
    private CompressionType compressionType;
    private TSEncoding encodingType;
    private int numOfPages;
    private String dataSample;
    private double[] numericVector = new double[7];

    public static SeriesFeatureVector fromChunkData(ChunkData data, int dataSampleLength) {
      ChunkHeader chunkHeader;
      Chunk chunk;
      ByteBuffer chunkBuffer;
      // sample a buffer from the chunk data
      if (data.isAligned()) {
        AlignedChunkData alignedChunkData = (AlignedChunkData) data;
        chunkHeader = alignedChunkData.getChunkHeaderList().get(0);
        if (!alignedChunkData.getChunkList().isEmpty()) {
          chunk = alignedChunkData.getChunkList().get(0);
          ByteBuffer buffer = chunk.getData();
          int sampleLength = Math.min(dataSampleLength, buffer.remaining());
          chunkBuffer = buffer.slice();
          chunkBuffer.limit(sampleLength);
        } else {
          chunkBuffer = ByteBuffer.wrap(alignedChunkData.getByteStream().getBuf());
          int sampleLength = Math.min(dataSampleLength, chunkBuffer.remaining());
          chunkBuffer.limit(sampleLength);
        }
      } else {
        NonAlignedChunkData nonAlignedChunkData = (NonAlignedChunkData) data;
        chunkHeader = nonAlignedChunkData.getChunkHeader();
        chunk = nonAlignedChunkData.getChunk();
        if (chunk != null) {
          ByteBuffer buffer = chunk.getData();
          int sampleLength = Math.min(dataSampleLength, buffer.remaining());
          chunkBuffer = buffer.slice();
          chunkBuffer.limit(sampleLength);
        } else {
          chunkBuffer = ByteBuffer.wrap(nonAlignedChunkData.getByteStream().getBuf());
          int sampleLength = Math.min(dataSampleLength, chunkBuffer.remaining());
          chunkBuffer.limit(sampleLength);
        }
      }

      SeriesFeatureVector vector = new SeriesFeatureVector();
      vector.measurementId = chunkHeader.getMeasurementID();
      vector.dataSize = chunkHeader.getDataSize();
      vector.dataType = chunkHeader.getDataType();
      vector.compressionType = chunkHeader.getCompressionType();
      vector.encodingType = chunkHeader.getEncodingType();
      vector.numOfPages = chunkHeader.getNumOfPages();
      vector.dataSample =
          new String(
              chunkBuffer.array(),
              chunkBuffer.arrayOffset() + chunkBuffer.position(),
              chunkBuffer.remaining());
      return vector;
    }

    public void mergeChunkData(ChunkData data) {
      ChunkHeader chunkHeader;
      if (data.isAligned()) {
        AlignedChunkData alignedChunkData = (AlignedChunkData) data;
        chunkHeader = alignedChunkData.getChunkHeaderList().get(0);
      } else {
        NonAlignedChunkData nonAlignedChunkData = (NonAlignedChunkData) data;
        chunkHeader = nonAlignedChunkData.getChunkHeader();
      }
      dataSize += chunkHeader.getDataSize();
      numOfPages += chunkHeader.getNumOfPages();
    }
  }

  public interface VectorDistance {

    double calculate(double[] v1, double[] v2);
  }

  public static class EuclideanDistance implements VectorDistance {

    @Override
    public double calculate(double[] v1, double[] v2) {
      double sum = 0;
      for (int i = 0; i < v1.length; i++) {
        sum += (v1[i] - v2[i]) * (v1[i] - v2[i]);
      }
      return Math.sqrt(sum) + 1;
    }
  }

  public interface Clustering {

    List<List<String>> cluster(Map<String, double[]> tagVectorMap, VectorDistance distance);
  }

  public class KMeans implements Clustering {

    private int clusterNum;
    private int maxIteration;
    private double[][] centroids;
    private AtomicInteger[] centroidCounters;
    private Random random;
    private Map<Entry<String, double[]>, Integer> recordCentroidMapping;
    private int vecLength = 0;

    public KMeans(int clusterNum, int maxIteration) {
      this.clusterNum = clusterNum;
      this.maxIteration = maxIteration;
      this.centroids = new double[clusterNum][];
      this.centroidCounters = new AtomicInteger[clusterNum];
      for (int i = 0; i < centroidCounters.length; i++) {
        centroidCounters[i] = new AtomicInteger();
      }
      this.random = new Random();
      this.recordCentroidMapping = new ConcurrentHashMap<>();
    }

    private void clearCentroidCounter() {
      for (AtomicInteger centroidCounter : centroidCounters) {
        centroidCounter.set(0);
      }
    }

    @Override
    public List<List<String>> cluster(Map<String, double[]> tagVectorMap, VectorDistance distance) {
      recordCentroidMapping.clear();
      if (clusterNum > tagVectorMap.size()) {
        clusterNum = tagVectorMap.size();
        this.centroids = new double[clusterNum][];
      }

      for (Entry<String, double[]> entry : tagVectorMap.entrySet()) {
        vecLength = entry.getValue().length;
      }

      randomCentroid(tagVectorMap);

      for (int i = 0; i < maxIteration && System.currentTimeMillis() - splitStartTime <= splitTimeBudget; i++) {
        if (!assignCentroid(tagVectorMap, distance)) {
          // centroid not updated, end
          break;
        }
        newCentroid();
        clearCentroidCounter();
      }

      Map<Integer, List<Entry<String, double[]>>> centroidRecordMap =
          collectCentroidRecordMapping();
      return centroidRecordMap.values().stream()
          .map(l -> l.stream().map(Entry::getKey).collect(Collectors.toList()))
          .collect(Collectors.toList());
    }

    private Map<Integer, List<Entry<String, double[]>>> collectCentroidRecordMapping() {
      Map<Integer, List<Entry<String, double[]>>> centroidRecordMapping = new ConcurrentHashMap<>();
      recordCentroidMapping.entrySet().stream()
          .parallel()
          .forEach(
              e ->
                  centroidRecordMapping.compute(
                      e.getValue(),
                      (key, oldV) -> {
                        if (oldV == null) {
                          oldV = new ArrayList<>();
                        }
                        oldV.add(e.getKey());
                        return oldV;
                      }));
      return centroidRecordMapping;
    }

    private void newCentroid() {
      Map<Integer, List<Entry<String, double[]>>> centroidRecordMapping =
          collectCentroidRecordMapping();
      centroidRecordMapping.entrySet().stream()
          .parallel()
          .forEach(
              e -> {
                Integer centroidId = e.getKey();
                List<Entry<String, double[]>> records = e.getValue();
                int recordNum = records.size();
                double[] sumVec = new double[vecLength];
                for (Entry<String, double[]> rec : records) {
                  for (int i = 0; i < sumVec.length; i++) {
                    sumVec[i] += rec.getValue()[i];
                  }
                }
                for (int i = 0; i < sumVec.length; i++) {
                  sumVec[i] = sumVec[i] / recordNum;
                }
                centroids[centroidId] = sumVec;
              });
    }

    private boolean assignCentroid(Map<String, double[]> tagVectorMap, VectorDistance distance) {
      AtomicBoolean centroidUpdated = new AtomicBoolean(false);
      tagVectorMap.entrySet().stream()
          .parallel()
          .forEach(
              e -> {
                double[] vector = e.getValue();
                double minDist = Double.MAX_VALUE;
                int nearestCentroid = 0;
                for (int i = 0; i < centroids.length; i++) {
                  double dist =
                      distance.calculate(vector, centroids[i])
                          * (1 + centroidCounters[i].get() * 0.1);
                  if (dist < minDist) {
                    minDist = dist;
                    nearestCentroid = i;
                  }
                }

                centroidCounters[nearestCentroid].incrementAndGet();
                int finalNearestCentroid = nearestCentroid;
                recordCentroidMapping.put(e, finalNearestCentroid);
                recordCentroidMapping.compute(
                    e,
                    (t, oc) -> {
                      if (oc == null || oc != finalNearestCentroid) {
                        centroidUpdated.set(true);
                        return finalNearestCentroid;
                      } else {
                        return oc;
                      }
                    });
              });
      return centroidUpdated.get();
    }

    private void randomCentroid(Map<String, double[]> tagVectorMap) {
      pickRandomCentroid(tagVectorMap);
    }

    private void pickRandomCentroid(Map<String, double[]> tagVectorMap) {
      List<double[]> recordVectors = new ArrayList<>(tagVectorMap.values());
      Collections.shuffle(recordVectors);
      for (int i = 0; i < clusterNum; i++) {
        centroids[i] = recordVectors.get(i);
      }
    }
  }
}
