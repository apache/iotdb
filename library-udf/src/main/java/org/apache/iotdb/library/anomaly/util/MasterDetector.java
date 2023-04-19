package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;

import java.math.BigDecimal;
import java.util.ArrayList;

public class MasterDetector {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> td_repaired = new ArrayList<>();
  private final ArrayList<Boolean> td_anomalies = new ArrayList<>();
  private final ArrayList<Boolean> anomalies_in_repaired = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Double> coeffs_one_column = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();
  private int[] initial_window;
  private final int columnCnt;
  private int k;
  private int p;
  private double[] std;
  private KDTreeUtil kdTreeUtil;

  private VAR prediction_model;

  private double eta;
  private double beta;

  public MasterDetector(int columnCnt, int k, int p, double eta, double beta) {
    this.columnCnt = columnCnt;
    this.k = k;
    this.p = p;
    this.eta = eta;
    this.beta = beta;
  }

  public boolean isNullRow(Row row) {
    boolean flag = true;
    for (int i = 0; i < row.size(); i++) {
      if (!row.isNull(i)) {
        flag = false;
        break;
      }
    }
    return flag;
  }

  public void addRow(Row row) throws Exception {
    ArrayList<Double> tt = new ArrayList<>(); // time-series tuple
    boolean containsNotNull = false;
    for (int i = 0; i < this.columnCnt; i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        BigDecimal bd = BigDecimal.valueOf(Util.getValueAsDouble(row, i));
        tt.add(bd.doubleValue());
      } else {
        tt.add(null);
      }
    }
    if (containsNotNull) {
      td.add(tt);
      td_time.add(row.getTime());
    }

    ArrayList<Double> mt = new ArrayList<>(); // master tuple
    containsNotNull = false;
    for (int i = this.columnCnt; i < this.columnCnt * 2; i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        BigDecimal bd = BigDecimal.valueOf(Util.getValueAsDouble(row, i));
        mt.add(bd.doubleValue());
      } else {
        mt.add(null);
      }
    }
    if (containsNotNull) {
      md.add(mt);
    }

    int i = this.columnCnt * 2;
    if (!row.isNull(i)) {
      coeffs_one_column.add(Util.getValueAsDouble(row, i));
    }
  }

  public void buildKDTree() {
    this.kdTreeUtil = new KDTreeUtil();
    this.kdTreeUtil.buildTree(this.md);
  }

  public double delta(ArrayList<Double> t_tuple, ArrayList<Double> m_tuple) {
    double distance = 0d;
    for (int pos = 0; pos < columnCnt; pos++) {
      double temp = t_tuple.get(pos) - m_tuple.get(pos);
      temp = temp / std[pos];
      distance += temp * temp;
    }
    distance = Math.sqrt(distance);
    return distance;
  }

  public void fillNullValue() {
    for (int i = 0; i < columnCnt; i++) {
      double temp = this.td.get(0).get(i);
      for (ArrayList<Double> arrayList : this.td) {
        if (arrayList.get(i) == null) {
          arrayList.set(i, temp);
        } else {
          temp = arrayList.get(i);
        }
      }
    }
  }

  private double varianceImperative(double[] value) {
    double average = 0.0;
    int cnt = 0;
    for (double p : value) {
      if (!Double.isNaN(p)) {
        cnt += 1;
        average += p;
      }
    }
    if (cnt == 0) {
      return 0d;
    }
    average /= cnt;

    double variance = 0.0;
    for (double p : value) {
      if (!Double.isNaN(p)) {
        variance += (p - average) * (p - average);
      }
    }
    return variance / cnt;
  }

  private double[] getColumn(int pos) {
    double[] column = new double[this.td.size()];
    for (int i = 0; i < this.td.size(); i++) {
      column[i] = this.td.get(i).get(pos);
    }
    return column;
  }

  public void call_std() {
    this.std = new double[this.columnCnt];
    for (int i = 0; i < this.columnCnt; i++) {
      std[i] = Math.sqrt(varianceImperative(getColumn(i)));
    }
  }

  public boolean checkConsistency(ArrayList<Double> tuple) {
    ArrayList<Double> NN = kdTreeUtil.findTheNearestNeighbor(tuple);
    double delta = delta(tuple, NN);
    if (delta > eta) {
      return false;
    } else return true;
  }

  public void getOriginalAnomaliesAndLearnModel() {
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Double> tuple = this.td.get(i);
      boolean isNormal = checkConsistency(tuple);
      td_anomalies.add(!isNormal);
    }
    this.prediction_model = new VAR(columnCnt);
    this.prediction_model.fitCoeffs(coeffs_one_column, columnCnt);
  }

  public void findInitialWindow(int p) {
    initial_window = new int[2];
    int left = 0;
    int right = 0;
    for (int i = 0; i < td_anomalies.size(); i++) {
      if (right - left + 1 == p) {
        initial_window[0] = left;
        initial_window[1] = right;
        break;
      }
      if (td_anomalies.get(i) == Boolean.TRUE) {
        left = i + 1;
        right = i + 1;
      } else {
        right++;
      }
    }
  }

  public ArrayList<ArrayList<Double>> getWindow(ArrayList<ArrayList<Double>> data, int i, int p) {
    if (i < p) {
      System.out.println("ERROR: i must be greater than p.");
      return new ArrayList<>();
    }
    ArrayList<ArrayList<Double>> W = new ArrayList<>();
    for (int j = i - p; j < i; j++) {
      W.add(data.get(j));
    }
    return W;
  }

  public double calForwardPredictionLoss(int i, int p, ArrayList<Double> candidate_for_i) {
    double sum_prediction_loss = 0.0;
    for (int index = 0; index < p; index++) {
      if (index == 0) {
        ArrayList<ArrayList<Double>> W_repaired = getWindow(this.td_repaired, i, p);
        ArrayList<Double> prediction_for_i = prediction_model.predict(W_repaired);
        sum_prediction_loss += delta(prediction_for_i, candidate_for_i);
      } else {
        if (td_anomalies.get(i + index) == Boolean.TRUE) {
          break;
        }

        ArrayList<ArrayList<Double>> W_repaired = getWindow(this.td_repaired, i, p - index);
        W_repaired.add(candidate_for_i);
        for (int j = 1; j < index; j++) {
          W_repaired.add(td.get(i + j));
        }
        ArrayList<Double> prediction_for_i = prediction_model.predict(W_repaired);
        sum_prediction_loss += delta(prediction_for_i, td.get(i + index));
      }
    }
    return sum_prediction_loss;
  }

  public double calBackwardPredictionLoss(int i, int p, ArrayList<Double> candidate_for_i) {
    double sum_prediction_loss = 0.0;
    for (int index = 0; index < p; index++) {
      if (index == 0) {
        ArrayList<ArrayList<Double>> W_repaired = getWindow(this.td_repaired, i + p, p);
        W_repaired.set(0, candidate_for_i);
        ArrayList<Double> prediction_for_i = prediction_model.predict(W_repaired);
        sum_prediction_loss += delta(prediction_for_i, td_repaired.get(i + p));
      } else {
        if (i - index < 0 || td_anomalies.get(i - index) == Boolean.TRUE) {
          break;
        }
        ArrayList<ArrayList<Double>> W_repaired = getWindow(this.td_repaired, i + p - index, p);
        W_repaired.set(index, candidate_for_i);
        ArrayList<Double> prediction_for_i = prediction_model.predict(W_repaired);
        sum_prediction_loss += delta(prediction_for_i, td_repaired.get(i + p));
      }
    }
    return sum_prediction_loss;
  }

  public void forwardRepairing(int p) {
    int i = initial_window[1] + 1;

    while (i < td.size()) {
      ArrayList<ArrayList<Double>> W_repaired = getWindow(this.td_repaired, i, p);
      ArrayList<Double> x_repaired_predicted = prediction_model.predict(W_repaired);
      ArrayList<Double> optimal_repair = new ArrayList<>();
      if (td_anomalies.get(i) == Boolean.TRUE) {
        ArrayList<ArrayList<Double>> candidates =
            this.kdTreeUtil.findKNearestNeighbors(x_repaired_predicted, this.k);
        // find the optimal repair
        double min_dis = Double.MAX_VALUE;
        for (ArrayList<Double> candidate : candidates) {
          double prediction_loss = calForwardPredictionLoss(i, p, candidate);
          if (prediction_loss < min_dis) {
            min_dis = prediction_loss;
            optimal_repair = candidate;
          }
        }
        this.td_repaired.add(optimal_repair);
      } else {
        optimal_repair = td.get(i);
        this.td_repaired.add(optimal_repair);
      }
      if (delta(x_repaired_predicted, optimal_repair) > beta) {
        this.anomalies_in_repaired.add(Boolean.TRUE);
      } else {
        this.anomalies_in_repaired.add(Boolean.FALSE);
      }
      i++;
    }
  }

  public void backwardRepairing(int p) {
    int i = initial_window[0] - 1;
    if (i < 0) {
      return;
    }

    while (i >= 0) {
      ArrayList<Double> optimal_repair = new ArrayList<>();
      if (td_anomalies.get(i) == Boolean.TRUE) {
        ArrayList<ArrayList<Double>> candidates =
            this.kdTreeUtil.findKNearestNeighbors(this.td_repaired.get(i + 1), k);
        double min_dis = Double.MAX_VALUE;
        for (ArrayList<Double> candidate : candidates) {
          double prediction_loss = calBackwardPredictionLoss(i, p, candidate);
          if (prediction_loss < min_dis) {
            min_dis = prediction_loss;
            optimal_repair = candidate;
          }
        }
        this.td_repaired.set(i, optimal_repair);
      } else {
        optimal_repair = td.get(i);
        this.td_repaired.set(i, optimal_repair);
      }

      if (delta(optimal_repair, td_repaired.get(i + p)) > beta) {
        this.anomalies_in_repaired.set(i, Boolean.TRUE);
      } else {
        this.anomalies_in_repaired.set(i, Boolean.FALSE);
      }
      i--;
    }
  }

  public void detectAndRepair() {
    fillNullValue();
    buildKDTree();
    call_std();
    ArrayList<Double> zero_tuple = new ArrayList<>();
    zero_tuple.add(0.0);
    zero_tuple.add(0.0);
    zero_tuple.add(0.0);
    for (int i = 0; i < 10; i++) {
      this.td.set(i, zero_tuple);
    }
    getOriginalAnomaliesAndLearnModel();
    findInitialWindow(p);
    System.out.println(initial_window[0] + " " + initial_window[1]);

    for (int j = 0; j <= initial_window[1]; j++) {
      this.td_repaired.add(this.td.get(j));
      this.anomalies_in_repaired.add(Boolean.FALSE);
    }

    backwardRepairing(p);
    forwardRepairing(p);
  }

  public ArrayList<ArrayList<Double>> getMd() {
    return md;
  }

  public ArrayList<ArrayList<Double>> getTd() {
    return td;
  }

  public ArrayList<Long> getTd_time() {
    return td_time;
  }

  public ArrayList<Double> getCoeffs_one_column() {
    return coeffs_one_column;
  }

  public ArrayList<ArrayList<Double>> getTd_repaired() {
    return td_repaired;
  }

  public ArrayList<Boolean> getAnomalies_in_repaired() {
    return anomalies_in_repaired;
  }
}
