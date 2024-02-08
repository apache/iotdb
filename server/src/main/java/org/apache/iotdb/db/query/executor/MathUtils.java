package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.tsfile.file.metadata.statistics.util.Complex;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.FFT;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.Jama.EigenvalueDecomposition;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.Jama.Matrix;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;

public class MathUtils {
  public static int[] extractTimeBound(Filter timeFilter) {
    int startTimeBound = Integer.MIN_VALUE, endTimeBound = Integer.MAX_VALUE;
    if (timeFilter != null) {
      String strTimeFilter = timeFilter.toString().replace(" ", "");
      strTimeFilter = strTimeFilter.replace("(", "").replace(")", "").replaceAll("time", "");
      String[] strTimes = strTimeFilter.split("&&");
      for (String strTime : strTimes) {
        if (strTime.contains(">=")) startTimeBound = Integer.parseInt(strTime.replaceAll(">=", ""));
        else if (strTime.contains(">"))
          startTimeBound = Integer.parseInt(strTime.replaceAll(">", "")) + 1;
        if (strTime.contains("<=")) endTimeBound = Integer.parseInt(strTime.replaceAll("<=", ""));
        else if (strTime.contains("<"))
          endTimeBound = Integer.parseInt(strTime.replaceAll("<", "")) - 1;
      }
    }
    return new int[] {startTimeBound, endTimeBound};
  }

  public static double[] _extractShape(double[][] sumMatrix, double[] oldCentroid) {
    int l = sumMatrix.length;
    Matrix s = new Matrix(sumMatrix);
    double[][] _p = new double[l][l];
    for (int u = 0; u < l; ++u) {
      for (int v = 0; v < l; ++v) {
        _p[u][v] = 1.0 / (l - 1);
      }
    }
    Matrix p = new Matrix(_p);
    p = Matrix.identity(l, l).minus(p);
    Matrix m = p.times(s).times(p);

    EigenvalueDecomposition E = new EigenvalueDecomposition(m);
    Matrix eigenVectors = E.getV();
    double[] eigenValues = E.getRealEigenvalues();
    int _maxIdx = -1;
    double _maxVal = Double.MIN_VALUE;
    for (int i = 0; i < l; ++i) {
      if (eigenValues[i] > _maxVal) {
        _maxVal = eigenValues[i];
        _maxIdx = i;
      }
    }
    double[] newCentroid = eigenVectors.transpose().getArray()[_maxIdx];

    double dis1 = 0.0, dis2 = 0.0;
    for (int i = 0; i < l; ++i) {
      dis1 += Math.pow(oldCentroid[i] - newCentroid[i], 2);
      dis2 += Math.pow(oldCentroid[i] + newCentroid[i], 2);
    }

    if (dis2 < dis1) {
      for (int i = 0; i < l; ++i) {
        newCentroid[i] = -newCentroid[i];
      }
    }
    return _zscore(newCentroid);
  }

  public static double _L2(double[] x1, double[] x2) {
    double res = 0.0;
    for (int i = 0; i < x1.length; i++) {
      res += Math.pow(x1[i] - x2[i], 2);
    }
    return Math.sqrt(res);
  }

  public static double _sbd(double[] x1, double[] x2) {
    double _maxv = Double.MIN_VALUE;
    for (double v : _ncc(x1, x2)) {
      if (v > _maxv) _maxv = v;
    }
    return 1 - _maxv / _norm(x1) / _norm(x2);
  }

  public static double[] _ncc(double[] x1, double[] x2) {
    double den = _norm(x1) * _norm(x2);
    if (den < 1e-9) den = Double.MAX_VALUE;
    int x_len = x1.length; // l
    int fft_size = (int) Math.pow(2, Integer.toBinaryString(2 * x_len - 1).length());
    double[] cc =
        FFT.ifft(Complex.multiply(FFT.fft(x1, fft_size), Complex.conjugate(FFT.fft(x2, fft_size))));
    double[] ncc = new double[fft_size - 1];
    // [-(x_len-1):] + [:x_len]
    for (int i = 0; i < fft_size - 1; i++)
      if (i < x_len - 1) ncc[i] = cc[cc.length - x_len + 1 + i] / den;
      else ncc[i] = cc[i - x_len + 1] / den;
    return ncc;
  }

  public static double _norm(double[] x) {
    double res = 0.0;
    for (double v : x) {
      res += v * v;
    }
    return Math.sqrt(res);
  }

  public static double[] _zscore(double[] X) {
    int l = X.length;
    double sum = 0.0;
    for (int j = 0; j < l; ++j) {
      sum += X[j];
    }
    double mean = sum / l;

    sum = 0.0;
    for (int j = 0; j < l; ++j) {
      sum += Math.pow(X[j] - mean, 2);
    }
    double std = Math.sqrt(sum / (l - 1));

    double[] res = new double[l];
    for (int j = 0; j < l; j++) {
      res[j] = (X[j] - mean) / std;
    }
    return res;
  }

  public static int clusterMemberNum(int[] idx, int label) {
    int cnt = 0;
    for (int id : idx) {
      if (id == label) cnt++;
    }
    return cnt;
  }

  public static void updateSumMatrices(double[][] matrix, List<Double> vector) {
    for (int i = 0; i < matrix.length; i++) {
      for (int j = 0; j < matrix[0].length; j++) {
        matrix[i][j] -= vector.get(i) * vector.get(j);
      }
    }
  }

  public static void updateEdCentroid(double[] edCentroid, int edCount, List<Double> vector) {
    for (int i = 0; i < edCentroid.length; i++) {
      edCentroid[i] = (edCentroid[i] * edCount - vector.get(i)) / (edCount - 1);
    }
  }
}
