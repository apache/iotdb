package org.apache.iotdb.tsfile.file.metadata.statistics.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class KMeans {
  private int k; // number of centroids
  private double[][] points; // n-dimensional data points.

  // optional parameters
  private int iterations; // number of times to repeat the clustering
  private boolean initialType; // true --> KMeans++. false --> basic random sampling
  private int n; // number of data points   (# of pixels for PhenoRipper)
  private int d; // number of dimensions    (# of channels for PhenoRipper)

  private double[][] centroids;
  private int[] idx;
  private double[] deltas;

  private KMeans() {}

  /**
   * The proper way to construct a KMeans object: from an inner class object.
   *
   * @param builder See inner class named Builder
   */
  private KMeans(Builder builder) {
    k = builder.k;
    points = builder.points;
    iterations = builder.iterations;
    initialType = builder.initialType;
    n = points.length;
    d = points[0].length;

    centroids = new double[k][d];
    idx = new int[n];
    deltas = new double[k];

    if (n < k) {
      for (int i = 0; i < n; i++) {
        centroids[i] = points[i];
        idx[i] = i;
      }
      return;
    }
    // run KMeans++ clustering algorithm
    run();
    calculateDeltas();
  }

  /**
   * Builder class for constructing KMeans objects.
   *
   * <p>For descriptions of the fields in this (inner) class, see outer class.
   */
  public static class Builder {
    // required
    private final int k;
    private final double[][] points;

    // optional (default values given)
    private int iterations = 10;
    private boolean initialType = true;

    /**
     * Sets required parameters and checks that are a sufficient # of distinct points to run KMeans.
     */
    public Builder(int k, double[][] points) {
      this.k = k;
      this.points = points;
    }

    /** Sets optional parameter. Default value is 50. */
    public Builder iterations(int iterations) {
      if (iterations < 1)
        throw new IllegalArgumentException("Required: non-negative number of iterations. Ex: 50");
      this.iterations = iterations;
      return this;
    }

    /** Sets optional parameter. Default value is true. */
    public Builder ifPlusInitial(boolean pp) {
      this.initialType = pp;
      return this;
    }

    /** Build a KMeans object */
    public KMeans build() {
      return new KMeans(this);
    }
  }

  /** Run KMeans algorithm */
  private void run() {
    int[] oldAssignment = new int[n];
    chooseInitialCentroids();
    for (int n = 0; n < iterations; n++) {
      assignmentStep(); // assign points to the closest centroids
      updateStep(); // update centroids
      if (oldAssignment == idx) break;
      oldAssignment = idx;
    }
  }

  private void assignmentStep() {
    idx = new int[n];
    double tempDist;
    double minValue;
    int minLocation;

    for (int i = 0; i < n; i++) {
      minLocation = 0;
      minValue = Double.POSITIVE_INFINITY;
      for (int j = 0; j < k; j++) {
        tempDist = Distance.L2(points[i], centroids[j]);
        if (tempDist < minValue) {
          minValue = tempDist;
          minLocation = j;
        }
      }
      idx[i] = minLocation;
    }
  }

  private void updateStep() {
    // reuse memory is faster than re-allocation
    for (int i = 0; i < k; i++) for (int j = 0; j < d; j++) centroids[i][j] = 0;

    int[] clustSize = new int[k];

    // sum points assigned to each cluster
    for (int i = 0; i < n; i++) {
      clustSize[idx[i]]++;
      for (int j = 0; j < d; j++) centroids[idx[i]][j] += points[i][j];
    }

    // store indices of empty clusters
    HashSet<Integer> emptyCentroids = new HashSet<Integer>();

    // divide to get averages -> centroids
    for (int i = 0; i < k; i++) {
      if (clustSize[i] == 0) emptyCentroids.add(i);
      else for (int j = 0; j < d; j++) centroids[i][j] /= clustSize[i];
    }

    // gracefully handle empty clusters by assigning to that centroid an unused data point
    if (emptyCentroids.size() != 0) {
      HashSet<double[]> nonemptyCentroids = new HashSet<double[]>(k - emptyCentroids.size());
      for (int i = 0; i < k; i++)
        if (!emptyCentroids.contains(i)) nonemptyCentroids.add(centroids[i]);

      Random r = new Random();
      for (int i : emptyCentroids)
        while (true) {
          int rand = r.nextInt(points.length);
          if (!nonemptyCentroids.contains(points[rand])) {
            nonemptyCentroids.add(points[rand]);
            centroids[i] = points[rand];
            break;
          }
        }
    }
  }

  /**
   * ********************************************************************* Choose initial centroids
   * ********************************************************************
   */
  /** Uses either plusplus (KMeans++) or a basic randoms sample to choose initial centroids */
  private void chooseInitialCentroids() {
    if (initialType) plusplus();
    else basicRandSample();
  }

  /** Randomly chooses (without replacement) k data points as initial centroids. */
  private void basicRandSample() {
    centroids = new double[k][d];
    double[][] copy = points;

    Random gen = new Random();

    int rand;
    for (int i = 0; i < k; i++) {
      rand = gen.nextInt(n - i);
      for (int j = 0; j < d; j++) {
        centroids[i][j] = copy[rand][j]; // store chosen centroid
        copy[rand][j] = copy[n - 1 - i][j]; // ensure sampling without replacement
      }
    }
  }

  /**
   * Randomly chooses (without replacement) k data points as initial centroids using a weighted
   * probability distribution (proportional to D(x)^2 where D(x) is the distance from a data point
   * to the nearest, already chosen centroid).
   */
  // TODO: see if some of this code is extraneous (can be deleted)
  private void plusplus() {
    centroids = new double[k][d];
    double[] distToClosestCentroid = new double[n];
    double[] weightedDistribution = new double[n]; // cumulative sum of squared distances

    Random random = new Random();
    int choose = 0;

    for (int c = 0; c < k; c++) {
      // first centroid: choose any data point
      if (c == 0) choose = random.nextInt(n);
      // after first centroid, use a weighted distribution
      else {
        // check if the most recently added centroid is closer to any of the points than previously
        // added ones
        for (int p = 0; p < n; p++) {
          // gives chosen points 0 probability of being chosen again -> sampling without replacement
          double tempDistance =
              Distance.L2(points[p], centroids[c - 1]); // need L2 norm here, not L1
          // base case: if we have only chosen one centroid so far, nothing to compare to
          if (c == 1) distToClosestCentroid[p] = tempDistance;
          else { // c != 1
            if (tempDistance < distToClosestCentroid[p]) distToClosestCentroid[p] = tempDistance;
          }
          // no need to square because the distance is the square of the euclidean dist
          if (p == 0) weightedDistribution[0] = distToClosestCentroid[0];
          else weightedDistribution[p] = weightedDistribution[p - 1] + distToClosestCentroid[p];
        }

        // choose the next centroid
        double rand = random.nextDouble();
        for (int j = n - 1; j > 0; j--) {
          // TODO: review and try to optimize
          // starts at the largest bin. EDIT: not actually the largest
          if (rand > weightedDistribution[j - 1] / weightedDistribution[n - 1]) {
            choose = j; // one bigger than the one above
            break;
          } else // Because of invalid dimension errors, we can't make the forloop go to j2 > -1
            // when we have (j2-1) in the loop.
            choose = 0;
        }
      }

      // store the chosen centroid
      for (int i = 0; i < d; i++) centroids[c][i] = points[choose][i];
    }
  }

  private static class Distance {
    public static double L1(double[] x, double[] y) {
      if (x.length != y.length) throw new IllegalArgumentException("dimension error");
      double dist = 0;
      for (int i = 0; i < x.length; i++) dist += Math.abs(x[i] - y[i]);
      return dist;
    }

    public static double L2(double[] x, double[] y) {
      if (x.length != y.length) throw new IllegalArgumentException("dimension error");
      double dist = 0;
      for (int i = 0; i < x.length; i++) dist += Math.abs((x[i] - y[i]) * (x[i] - y[i]));
      return dist;
    }
  }

  public int[] getIdx() {
    return idx;
  }

  public double[][] getCentroids() {
    return centroids;
  }

  public double[] getDeltas() {
    return deltas;
  }

  public void calculateDeltas() {
    double[] res = new double[k];
    int[] cnt = new int[k];
    double[][] X = this.points;
    for (int i = 0; i < X.length; i++) {
      res[idx[i]] += Distance.L2(X[i], this.centroids[idx[i]]);
      cnt[idx[i]] += 1;
    }
    for (int i = 0; i < k; i++) {
      if (cnt[i] != 0) res[i] /= cnt[i];
      else res[i] = -1;
    }
    this.deltas = res;
  }

  public static void main(String args[]) throws IOException {
    int k = 2;
    double[][] points = new double[][] {{6, 4}, {6, 6}, {6, 7}, {-3, 2}, {-3, 0}};

    // run K-means
    KMeans clustering = new KMeans.Builder(k, points).iterations(50).ifPlusInitial(true).build();
    System.out.println(Arrays.deepToString(clustering.getCentroids()));
    System.out.println(Arrays.toString(clustering.getIdx()));
  }
}
