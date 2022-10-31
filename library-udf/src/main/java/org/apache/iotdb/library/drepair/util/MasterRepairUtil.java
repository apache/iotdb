package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

public class MasterRepairUtil {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> td_cleaned = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();
  private final int columnCnt;
  private long omega;
  private Double eta;
  private int k;
  private double[] std;
  private KDTreeUtil kdTreeUtil;
  private int repaired_cnt;
  private int total_cnt;

  public MasterRepairUtil(int columnCnt, long omega, double eta, int k) throws Exception {
    this.columnCnt = columnCnt;
    this.omega = omega;
    this.eta = eta;
    this.k = k;
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
    this.kdTreeUtil = KDTreeUtil.build(md, this.columnCnt);
  }

  public ArrayList<Double> getCleanResultColumn(int columnPos) {
    ArrayList<Double> column = new ArrayList<>();
    for (ArrayList<Double> tuple : this.td_cleaned) {
      column.add(tuple.get(columnPos - 1));
    }
    return column;
  }

  public ArrayList<Long> getTime() {
    return td_time;
  }

  public double get_tm_distance(ArrayList<Double> t_tuple, ArrayList<Double> m_tuple) {
    double distance = 0d;
    for (int pos = 0; pos < columnCnt; pos++) {
      double temp = t_tuple.get(pos) - m_tuple.get(pos);
      temp = temp / std[pos];
      distance += temp * temp;
    }
    distance = Math.sqrt(distance);
    return distance;
  }

  public ArrayList<Integer> cal_W(int i) {
    ArrayList<Integer> W_i = new ArrayList<>();
    for (int l = i - 1; l >= 0; l--) {
      if (this.td_time.get(i) <= this.td_time.get(l) + omega) {
        W_i.add(l);
      } else {
        break;
      }
    }
    return W_i;
  }

  public ArrayList<ArrayList<Double>> cal_C(int i, ArrayList<Integer> W_i) {
    ArrayList<ArrayList<Double>> C_i = new ArrayList<>();
    if (W_i.size() == 0) {
      C_i.add(this.kdTreeUtil.query(this.td.get(i), std));
    } else {
      C_i.addAll(this.kdTreeUtil.queryKNN(this.td.get(i), k, std));
      for (Integer integer : W_i) {
        C_i.addAll(this.kdTreeUtil.queryKNN(this.td_cleaned.get(integer), k, std));
      }
    }
    return C_i;
  }

  public void master_repair() {
    this.repaired_cnt = 0;
    this.total_cnt = 0;
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Double> tuple = this.td.get(i);
      ArrayList<Integer> W_i = cal_W(i);
      ArrayList<ArrayList<Double>> C_i = this.cal_C(i, W_i);
      double min_dis = Double.MAX_VALUE;
      ArrayList<Double> repair_tuple = new ArrayList<>();
      for (ArrayList<Double> c_i : C_i) {
        boolean smooth = true;
        for (Integer w_i : W_i) {
          ArrayList<Double> w_is = td_cleaned.get(w_i);
          if (get_tm_distance(c_i, w_is) > eta) {
            smooth = false;
            break;
          }
        }
        if (smooth) {
          double dis = get_tm_distance(c_i, tuple);
          if (dis < min_dis) {
            min_dis = dis;
            repair_tuple = c_i;
          }
        }
      }
      if (!repair_tuple.toString().equals(tuple.toString())) {
        repaired_cnt++;
      }
      total_cnt++;
      this.td_cleaned.add(repair_tuple);
    }
  }

  public void set_parameters() {
    if (omega == -1) {
      ArrayList<Long> intervals = getIntervals();
      Collections.sort(intervals);
      long interval = intervals.get(intervals.size() / 2);
      omega = interval * 10;
    }
    if (Double.isNaN(eta)) {
      ArrayList<Double> distance_list = new ArrayList<>();
      for (int i = 1; i < this.td.size(); i++) {
        for (int l = i - 1; l >= 0; l--) {
          if (this.td_time.get(i) <= this.td_time.get(l) + omega) {
            distance_list.add(get_tm_distance(this.td.get(i), this.td.get(l)));
          } else break;
        }
      }
      Collections.sort(distance_list);
      eta = distance_list.get((int) (distance_list.size() * 0.9973));
    }
    if (k == -1) {
      for (int temp_k = 2; temp_k <= 5; temp_k++) {
        ArrayList<Double> distance_list = new ArrayList<>();
        for (ArrayList<Double> tuple : this.td) {
          ArrayList<ArrayList<Double>> neighbors = this.kdTreeUtil.queryKNN(tuple, temp_k, std);
          for (ArrayList<Double> neighbor : neighbors) {
            distance_list.add(get_tm_distance(tuple, neighbor));
          }
        }
        Collections.sort(distance_list);
        if (distance_list.get((int) (distance_list.size() * 0.9)) > eta) {
          k = temp_k;
          break;
        }
      }
      if (k == -1) {
        k = 5;
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

  public void repair() {
    fillNullValue();
    buildKDTree();
    call_std();
    set_parameters();
    master_repair();
  }

  public ArrayList<Long> getIntervals() {
    ArrayList<Long> intervals = new ArrayList<>();
    for (int i = 1; i < this.td_time.size(); i++) {
      intervals.add(this.td_time.get(i) - this.td_time.get(i - 1));
    }
    return intervals;
  }

  public int getRepaired_cnt() {
    return repaired_cnt;
  }

  public int getTotal_cnt() {
    return total_cnt;
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
}
