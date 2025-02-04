package org.apache.iotdb.library.dquality.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeSeriesSegment {
  protected int n;
  protected long[] time;
  protected double lmdA = 100000.0; // Assign appropriate value for lmdA
  protected double lmdD = 100000.0; // Assign appropriate value for lmdD
  protected double mate = 100000.0; // Assign appropriate value for mate
  protected long[] starts;
  protected long[] ends;

  public TimeSeriesSegment(RowIterator dataIterator) throws Exception {
    ArrayList<Long> timeList = new ArrayList<>();
    while (dataIterator.hasNextRow()) {
      Row row = dataIterator.next();
      timeList.add(row.getTime());
    }
    time = Util.toLongArray(timeList);
    n = time.length;
    timeList.clear();
  }

  public List<Map.Entry<Double, Long>> modeIntervalGranularity(List<Double> value) {
    Map<Double, Long> counter =
        value.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
    return counter.entrySet().stream()
        .sorted(Map.Entry.<Double, Long>comparingByValue().reversed())
        .collect(Collectors.toList());
  }

  public double move(int i, int j, double interval, int s0) {
    double tLen = time[j] - time[s0];
    double sLen = i * interval;
    double m = Math.abs(tLen - sLen) / interval;
    return (m == 0) ? mate : -1 * m;
  }

  public Map<String, Object> scoreMatrix(double epsT, int k) {
    int sNum = (int) Math.round((time[time.length - 1] - time[0]) / epsT + 1);
    double[][] dp = new double[sNum][time.length];
    int[][] st = new int[sNum][time.length];
    int[][] step = new int[sNum][time.length];
    List<Integer> s0 = new ArrayList<>();
    for (int i = 0; i < sNum; i++) {
      for (int j = 0; j < time.length; j++) {
        dp[i][j] = -100;
        st[i][j] = 0;
      }
    }
    for (int j = 0; j < time.length; j++) {
      dp[0][j] = mate;
      st[0][j] = j;
      step[0][j] = 2;
      if (j != 0 && Math.abs(time[j] - time[j - 1]) > (k * epsT)) {
        s0.add(j);
      }
    }
    for (int i = 1; i < sNum; i++) {
      for (int j = 0; j < time.length; j++) {
        if (j == 0) {
          dp[i][0] = Math.round(dp[i - 1][0] - lmdA);
          step[i][0] = 1;
          continue;
        }
        Map<Integer, Integer> dic = new HashMap<>();
        dic.put(0, st[i - 1][j - 1]);
        dic.put(1, st[i - 1][j]);
        dic.put(2, st[i][j - 1]);
        if (s0.contains(j)) {
          dp[i][j] = Math.round(dp[i - 1][j] - lmdA);
          st[i][j] = st[i - 1][j];
        } else {
          double m = Math.round(move(i, j, epsT, st[i - 1][j - 1]));
          double a = Math.round(dp[i - 1][j] - lmdA);
          double d = Math.round(dp[i][j - 1] - lmdD);
          double[] arr = {Math.round((dp[i - 1][j - 1] + m)), a, d};
          dp[i][j] = Math.round(Arrays.stream(arr).max().orElse(0.0));

          for (int kIndex = 0; kIndex < arr.length; kIndex++) {
            if (arr[kIndex] == dp[i][j]) {
              step[i][j] = kIndex;
              break;
            }
          }
          st[i][j] = dic.get(step[i][j]);
        }
      }
    }
    Map<String, Object> result = new HashMap<>();
    result.put("dp", dp);
    result.put("st", st);
    result.put("sNum", sNum);
    result.put("step", step);
    s0.clear();
    return result;
  }

  public void exactRepair() {
    List<Double> epsList = new ArrayList<>();
    for (int i = 1; i < time.length; i++) {
      epsList.add((double) Math.round(time[i] - time[i - 1]));
    }
    List<Map.Entry<Double, Long>> interval = modeIntervalGranularity(epsList);
    lmdA = interval.get(0).getKey();
    lmdD = interval.get(0).getKey();
    mate = interval.get(0).getKey() * 2;

    double median =
        epsList.stream().mapToDouble(Double::doubleValue).sorted().toArray()[epsList.size() / 2];
    int tNum = (median == interval.get(0).getKey()) ? 1 : 3;

    List<List<Double>> intervalList = new ArrayList<>();
    double[][] allMatrix = null;
    int[][] allStart = null;
    int[][] allStep = null;
    int i = 0;
    for (int j = 0; j < tNum; j++) {
      double epsT = interval.get(j).getKey();

      if (epsT == 0) {
        continue;
      }
      Map<String, Object> result = scoreMatrix(epsT, 10);
      double[][] mt = (double[][]) result.get("dp");
      int[][] st = (int[][]) result.get("st");
      int m = (int) result.get("sNum");
      int[][] step = (int[][]) result.get("step");
      i = i + 1;
      for (int k = 1; k <= m; k++) {
        intervalList.add(Arrays.asList(epsT, (double) k));
      }
      if (i == 1) {
        allMatrix = mt;
        allStart = st;
        allStep = step;
        continue;
      } else {
        // Concatenate all_matrix and mt
        double[][] concatenatedMatrix = new double[mt.length + allMatrix.length][];
        System.arraycopy(allMatrix, 0, concatenatedMatrix, 0, allMatrix.length);
        System.arraycopy(mt, 0, concatenatedMatrix, allMatrix.length, mt.length);
        allMatrix = concatenatedMatrix;
        int[][] concatenate = new int[st.length + allStart.length][];
        System.arraycopy(allStart, 0, concatenate, 0, allStart.length);
        System.arraycopy(st, 0, concatenate, allStart.length, st.length);
        allStart = concatenate;

        int[][] concatenatestep = new int[step.length + allStep.length][];
        System.arraycopy(allStep, 0, concatenatestep, 0, allStep.length);
        System.arraycopy(step, 0, concatenatestep, allStep.length, step.length);
        allStep = concatenatestep;
      }
    }
    section(allMatrix, allStart, allStep, intervalList);
  }

  public void section(
      double[][] matrix, int[][] start, int[][] step, List<List<Double>> intervalList) {
    List<Integer> s0e = new ArrayList<>();
    List<Integer> end = new ArrayList<>();
    int j = time.length - 1;
    while (j >= 0) {

      double maxValue = Double.NEGATIVE_INFINITY;
      int maxRowIndex = -1;

      for (int rowIndex = 0; rowIndex < matrix.length; rowIndex++) {
        if (matrix[rowIndex][j] > maxValue) {
          maxValue = matrix[rowIndex][j];
          maxRowIndex = rowIndex;
        }
      }
      s0e.add(start[maxRowIndex][j]);
      end.add(j);
      j = start[maxRowIndex][j] - 1;
    }
    starts = new long[s0e.size()];
    ends = new long[end.size()];
    for (int i = 0; i < s0e.size(); i++) {
      starts[i] = s0e.get(i);
      ends[i] = end.get(i);
    }
    s0e.clear();
    end.clear();
  }

  public long[] getStart() {
    return starts;
  }

  public long[] getEnd() {
    return ends;
  }
}
