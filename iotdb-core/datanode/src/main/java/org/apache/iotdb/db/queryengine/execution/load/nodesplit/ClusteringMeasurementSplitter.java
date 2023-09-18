package org.apache.iotdb.db.queryengine.execution.load.nodesplit;

import java.util.ArrayList;
import java.util.Collection;
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

public class ClusteringMeasurementSplitter implements PieceNodeSplitter {

  private int numCluster;
  private int maxIteration;

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

    Map<String, LoadTsFilePieceNode> measurementPieceNodeMap = new HashMap<>();

    for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
      ChunkData chunkData = (ChunkData) tsFileData;
      String currMeasurement = chunkData.firstMeasurement();
      LoadTsFilePieceNode pieceNodeSplit = measurementPieceNodeMap.computeIfAbsent(
          currMeasurement,
          m -> new LoadTsFilePieceNode(pieceNode.getPlanNodeId(), pieceNode.getTsFile()));
      pieceNodeSplit.addTsFileData(chunkData);
    }

    return clusterPieceNode(measurementPieceNodeMap);
  }

  private List<LoadTsFilePieceNode> clusterPieceNode(
      Map<String, LoadTsFilePieceNode> measurementPieceNodeMap) {
    Map<String, SeriesFeatureVector> measurementVectorMap = new HashMap<>(
        measurementPieceNodeMap.size());
    for (Entry<String, LoadTsFilePieceNode> entry : measurementPieceNodeMap.entrySet()) {
      measurementVectorMap.put(entry.getKey(), convertToFeature(entry.getValue()));
    }
    normalize(measurementVectorMap.values());
    Map<String, double[]> doubleVectors = new HashMap<>(measurementPieceNodeMap.size());
    for (Entry<String, SeriesFeatureVector> e : measurementVectorMap.entrySet()) {
      doubleVectors.put(e.getKey(), e.getValue().numericVector);
    }

    VectorDistance distance = new EuclideanDistance();
    Clustering clustering = new KMeans(numCluster, maxIteration);
    List<List<String>> clusterResult = clustering.cluster(doubleVectors, distance);

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
    for (SeriesFeatureVector vector : vectors) {
      if (firstMeasurementId == null) {
        firstMeasurementId = vector.measurementId;
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
    }
  }

  private SeriesFeatureVector convertToFeature(LoadTsFilePieceNode pieceNode) {
    List<TsFileData> allTsFileData = pieceNode.getAllTsFileData();
    SeriesFeatureVector vector = null;
    for (TsFileData tsFileData : allTsFileData) {
      ChunkData chunkData = (ChunkData) tsFileData;
      if (chunkData.isAligned()) {
        AlignedChunkData alignedChunkData = (AlignedChunkData) chunkData;
        List<ChunkHeader> chunkHeaderList = alignedChunkData.getChunkHeaderList();

        for (ChunkHeader header : chunkHeaderList) {
          if (vector == null) {
            vector = SeriesFeatureVector.fromChunkHeader(header);
          } else {
            vector.mergeChunkHeader(header);
          }
        }
      } else {
        NonAlignedChunkData nonAlignedChunkData = (NonAlignedChunkData) chunkData;
        ChunkHeader header = nonAlignedChunkData.getChunkHeader();
        if (vector == null) {
          vector = SeriesFeatureVector.fromChunkHeader(header);
        } else {
          vector.mergeChunkHeader(header);
        }
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
    private double[] numericVector = new double[6];

    public static SeriesFeatureVector fromChunkHeader(ChunkHeader header) {
      SeriesFeatureVector vector = new SeriesFeatureVector();
      vector.measurementId = header.getMeasurementID();
      vector.dataSize = header.getDataSize();
      vector.dataType = header.getDataType();
      vector.compressionType = header.getCompressionType();
      vector.encodingType = header.getEncodingType();
      vector.numOfPages = header.getNumOfPages();
      return vector;
    }

    public void mergeChunkHeader(ChunkHeader header) {
      dataSize += header.getDataSize();
      numOfPages += header.getNumOfPages();
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

      for (Entry<String, double[]> entry : tagVectorMap.entrySet()) {
        vecLength = entry.getValue().length;
      }

      for (int i = 0; i < k; i++) {
        centroids[i] = randomCentroid(vecLength);
      }

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

    private double[] randomCentroid(int vecLength) {
      double[] centroid = new double[vecLength];
      for (int i = 0; i < vecLength; i++) {
        centroid[i] = random.nextDouble();
      }
      return centroid;
    }
  }
}
