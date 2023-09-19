package org.apache.iotdb.db.queryengine.execution.load.nodesplit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import net.ricecode.similarity.JaroWinklerStrategy;
import net.ricecode.similarity.SimilarityStrategy;
import net.ricecode.similarity.StringSimilarityService;
import net.ricecode.similarity.StringSimilarityServiceImpl;
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

public class ClusteringMeasurementSplitter implements PieceNodeSplitter {

  private int numCluster;
  private int maxIteration;
  private int dataSampleLength = 128;

  public ClusteringMeasurementSplitter(int numCluster, int maxIteration) {
    this.numCluster = numCluster;
    this.maxIteration = maxIteration;
  }

  @Override
  public List<LoadTsFilePieceNode> split(LoadTsFilePieceNode pieceNode) {
    if (pieceNode.isHasModification()) {
      // the order of modifications should be preserved, so with modifications clustering cannot be used
      return new OrderedMeasurementSplitter().split(pieceNode);
    }

    // split by measurement first
    Map<String, LoadTsFilePieceNode> measurementPieceNodeMap = new HashMap<>();
    for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
      ChunkData chunkData = (ChunkData) tsFileData;
      String currMeasurement = chunkData.firstMeasurement();
      LoadTsFilePieceNode pieceNodeSplit = measurementPieceNodeMap.computeIfAbsent(
          currMeasurement,
          m -> new LoadTsFilePieceNode(pieceNode.getPlanNodeId(), pieceNode.getTsFile()));
      pieceNodeSplit.addTsFileData(chunkData);
    }
    // use clustering to merge similar measurements
    return clusterPieceNode(measurementPieceNodeMap);
  }

  private List<LoadTsFilePieceNode> clusterPieceNode(
      Map<String, LoadTsFilePieceNode> measurementPieceNodeMap) {
    // convert to feature vector
    Map<String, SeriesFeatureVector> measurementVectorMap = new HashMap<>(
        measurementPieceNodeMap.size());
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
      LoadTsFilePieceNode clusterNode = new LoadTsFilePieceNode(pieceNode.getPlanNodeId(),
          pieceNode.getTsFile());
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

    SimilarityStrategy strategy = new JaroWinklerStrategy();
    StringSimilarityService service = new StringSimilarityServiceImpl(strategy);
    for (SeriesFeatureVector vector : vectors) {
      vector.numericVector[0] = service.score(firstMeasurementId, vector.measurementId);
      vector.numericVector[1] = (vector.dataSize - minDataSize) * 1.0 / (maxDataSize - minDataSize);
      vector.numericVector[2] =
          (vector.dataType.ordinal() - minDataType) * 1.0 / (maxDataType - minDataType);
      vector.numericVector[3] =
          (vector.compressionType.ordinal() - minCompressionType) * 1.0 / (maxCompressionType
              - minCompressionType);
      vector.numericVector[4] =
          (vector.encodingType.ordinal() - minEncodingType) * 1.0 / (maxEncodingType
              - minEncodingType);
      vector.numericVector[5] =
          (vector.numOfPages - minNumOfPages) * 1.0 / (maxNumOfPages - minNumOfPages);
      vector.numericVector[6] = service.score(firstDataSample, vector.dataSample);
      double[] numericVector = vector.numericVector;
      for (int i = 0; i < numericVector.length; i++) {
        if (Double.isNaN(numericVector[i])) {
          numericVector[i] = 0.0;
        } else if (Double.isInfinite(numericVector[i])) {
          numericVector[i] = 1.0;
        }
      }
    }
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
      vector.dataSample = new String(chunkBuffer.array(),
          chunkBuffer.arrayOffset() + chunkBuffer.position(), chunkBuffer.remaining());
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
        sum += Math.pow(v1[i] - v2[i], 2);
      }
      return Math.sqrt(sum);
    }
  }

  public interface Clustering {

    List<List<String>> cluster(Map<String, double[]> tagVectorMap, VectorDistance distance);
  }

  public static class KMeans implements Clustering {

    private int k;
    private int maxIteration;
    private double[][] centroids;
    private Random random;
    private Map<Entry<String, double[]>, Integer> recordCentroidMapping;
    private int vecLength = 0;

    public KMeans(int k, int maxIteration) {
      this.k = k;
      this.maxIteration = maxIteration;
      this.centroids = new double[k][];
      this.random = new Random();
      this.recordCentroidMapping = new ConcurrentHashMap<>();
    }

    @Override
    public List<List<String>> cluster(Map<String, double[]> tagVectorMap, VectorDistance distance) {
      recordCentroidMapping.clear();
      if (k > tagVectorMap.size()) {
        k = tagVectorMap.size();
        this.centroids = new double[k][];
      }

      for (Entry<String, double[]> entry : tagVectorMap.entrySet()) {
        vecLength = entry.getValue().length;
      }

      randomCentroid(vecLength, tagVectorMap);

      for (int i = 0; i < maxIteration; i++) {
        if (!assignCentroid(tagVectorMap, distance)) {
          break;
        }
        newCentroid();
      }

      Map<Integer, List<Entry<String, double[]>>> centroidRecordMap = collectCentroidRecordMapping();
      return centroidRecordMap.values().stream()
          .map(l -> l.stream().map(Entry::getKey).collect(Collectors.toList())).collect(
              Collectors.toList());
    }

    private Map<Integer, List<Entry<String, double[]>>> collectCentroidRecordMapping() {
      Map<Integer, List<Entry<String, double[]>>> centroidRecordMapping = new ConcurrentHashMap<>();
      recordCentroidMapping.entrySet().stream().parallel()
          .forEach(e -> centroidRecordMapping.compute(e.getValue(), (key, oldV) -> {
            if (oldV == null) {
              oldV = new ArrayList<>();
            }
            oldV.add(e.getKey());
            return oldV;
          }));
      return centroidRecordMapping;
    }

    private void newCentroid() {
      Map<Integer, List<Entry<String, double[]>>> centroidRecordMapping = collectCentroidRecordMapping();
      centroidRecordMapping.entrySet().stream().parallel().forEach(e -> {
        Integer centroidId = e.getKey();
        List<Entry<String, double[]>> records = e.getValue();
        int recordNum = records.size();
        double[] sumVec = new double[vecLength];
        for (Entry<String, double[]> record : records) {
          for (int i = 0; i < sumVec.length; i++) {
            sumVec[i] += record.getValue()[i];
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
      tagVectorMap.entrySet().stream().parallel().forEach(e -> {
        double[] vector = e.getValue();
        double minDist = Double.MAX_VALUE;
        int nearestCentroid = 0;
        for (int i = 0; i < centroids.length; i++) {
          double dist = distance.calculate(vector, centroids[i]);
          if (dist < minDist) {
            minDist = dist;
            nearestCentroid = i;
          }
        }

        int finalNearestCentroid = nearestCentroid;
        recordCentroidMapping.compute(e, (t, oc) -> {
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

    private void randomCentroid(int vecLength, Map<String, double[]> tagVectorMap) {
      pickRandomCentroid(tagVectorMap);
      // genRandomCentroid(vecLength);
    }

    private void pickRandomCentroid(Map<String, double[]> tagVectorMap) {
      List<double[]> recordVectors = tagVectorMap.values().stream().collect(Collectors.toList());
      Collections.shuffle(recordVectors);
      for (int i = 0; i < k; i++) {
        centroids[i] = recordVectors.get(i);
      }
    }

    private void genRandomCentroid(int vecLength) {
      for (int i = 0; i < k; i++) {
        double[] centroid = new double[vecLength];
        for (int j = 0; j < vecLength; j++) {
          centroid[j] = random.nextDouble();
        }
        centroids[i] = centroid;
      }
    }
  }
}
