package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;

import java.math.BigDecimal;
import java.util.ArrayList;

public class MasterTrainUtil {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();
  private final int columnCnt;
  private int p;
  private double[] std;
  private KDTreeUtil kdTreeUtil;

  private VAR prediction_model;

  private double eta;

  public MasterTrainUtil(int columnCnt, int p, double eta) {
    this.columnCnt = columnCnt;
    this.p = p;
    this.eta = eta;
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
    for (int i = this.columnCnt; i < row.size(); i++) {
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

  public void getOriginalAnomaliesAndTrainModel() {
    int left = 0;
    int right = 0;
    ArrayList<ArrayList<Double>> learning_samples = new ArrayList<>();
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Double> tuple = this.td.get(i);
      boolean isNormal = checkConsistency(tuple);
      if (right - left + 1 == p) {
        for (int j = left; j <= right; j++) {
          learning_samples.add(this.td.get(j));
        }
        left = i + 1;
        right = i + 1;
        continue;
      }
      if (isNormal) {
        right++;
      } else {
        left = i + 1;
        right = i + 1;
      }
    }
    this.prediction_model = new VAR(columnCnt);
    this.prediction_model.fit(learning_samples);
  }

  public void train() {
    fillNullValue();
    buildKDTree();
    call_std();
    getOriginalAnomaliesAndTrainModel();
  }

  public ArrayList<Double> coeffsInOneColumn() {
    ArrayList<ArrayList<Double>> coeffs = this.prediction_model.getCoeffs();
    ArrayList<Double> coeffs_one_column = new ArrayList<>();
    for (ArrayList<Double> coeff : coeffs) {
      coeffs_one_column.addAll(coeff);
    }
    return coeffs_one_column;
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
}
