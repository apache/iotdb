package apache.iotdb.quality.alignment;


import org.apache.commons.math3.analysis.function.Min;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.apache.commons.math3.util.Combinations;

import java.io.File;
import java.sql.Array;
import java.text.SimpleDateFormat;
import java.util.*;

public class AlignmentExecuter {

  private ArrayList<ArrayList<Long>> timeArrayList;
  private ArrayList<ArrayList<Double>> valueArrayList;
  private ArrayList<Long> resultTimeList = new ArrayList<>();
  private ArrayList<String> resultValueList = new ArrayList<>();
  private double delta = 50;
  private int theta = 100000; // 100000ms = 100s
  private int dim; // dimension
  private int rho = 2; // optimization parameter
  public Model model = new Model();


  public AlignmentExecuter(ArrayList<ArrayList<Long>> timeArrayList_, ArrayList<ArrayList<Double>> valueArrayList_){
    this.timeArrayList = timeArrayList_;
    this.valueArrayList = valueArrayList_;
    this.dim = timeArrayList_.size();
  }

  public AlignmentExecuter(ArrayList<ArrayList<Long>> timeArrayList_, ArrayList<ArrayList<Double>> valueArrayList_, double delta_, int theta_){
    this.timeArrayList = timeArrayList_;
    this.valueArrayList = valueArrayList_;
    this.dim = timeArrayList_.size();
    this.delta = delta_;
    this.theta = theta_;
  }

  public AlignmentExecuter(String[] fileList) throws Exception {
    this.timeArrayList = new ArrayList<>();
    this.valueArrayList = new ArrayList<>();
    for (String filename: fileList) {
      ArrayList<Long> timeArray = new ArrayList<>();
      ArrayList<Double> valueArray = new ArrayList<>();

      Scanner sc = new Scanner(new File(filename));
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      sc.useDelimiter("\\s*(,|\\r|\\n)\\s*");
      sc.nextLine();
      while (sc.hasNext()) {
        long t = format.parse(sc.next()).getTime();
        double v = sc.nextDouble();
        timeArray.add(t);
        valueArray.add(v);
      }
      this.timeArrayList.add(timeArray);
      this.valueArrayList.add(valueArray);
    }
    this.dim = fileList.length;
  }

  public ArrayList<ArrayList<Integer>> generateTimeCandidate() {
    ArrayList<ArrayList<Integer>> candidateIndexList = new ArrayList<>();
    for (int x = 0; x < dim; x++) {
      int[] pointer_start = new int[dim];
      int[] pointer_end = new int[dim];
      Arrays.fill(pointer_start, 0);
      Arrays.fill(pointer_end, 0);
      for (int idx = 0; idx < timeArrayList.get(x).size(); idx++) {
        long ts = timeArrayList.get(x).get(idx);
        boolean stop_flag = false;
        ArrayList<ArrayList<Integer>> candidateIndexListTmp = new ArrayList<>();
        candidateIndexListTmp.add(new ArrayList<>());
        // for each y, compute the timestamps in threshold
        for (int y = 0; y < dim; y++) {
          if (y == x) {
            for (ArrayList<Integer> candidate: candidateIndexListTmp) {
              candidate.add(idx);
            }
            continue;
          }
          int start = pointer_start[y];
          int end = pointer_end[y];
          int length = timeArrayList.get(y).size();
          while (start < length && timeArrayList.get(y).get(start) < ts) {
            start++;
          }
          while (end < length && timeArrayList.get(y).get(end) < ts + theta) {
            end++;
          }
          pointer_start[y] = start;
          pointer_end[y] = end;
          if (start >= length) {
            stop_flag = true;
          }

          ArrayList<ArrayList<Integer>> newList = new ArrayList<>();
          for (int ptr = start; ptr < end; ptr++){
            for (ArrayList<Integer> candidate: candidateIndexListTmp) {
              ArrayList<Integer> candidateNew = (ArrayList<Integer>)candidate.clone();
              candidateNew.add(ptr);
              newList.add(candidateNew);
            }
          }
          candidateIndexListTmp = newList;
        }
        for (ArrayList<Integer> candidate: candidateIndexListTmp) {
          candidateIndexList.add(candidate);
        }
        if (stop_flag) {
          break;
        }
      }
    }
    Collections.sort(candidateIndexList, (a, b) -> {
      int minSize = Math.min(a.size(), b.size());
      for (int i = 0; i < minSize; i++) {
        if (a.get(i).compareTo(b.get(i)) != 0) {
          return a.get(i).compareTo(b.get(i));
        }
      }
      return 0;
    });
    return candidateIndexList;
  }

  public void alignmentSearch() {
    ArrayList<ArrayList<Integer>> candidates = generateTimeCandidate();

    // model constraint
    if (model.enable) {
      for (int idx = 0; idx < candidates.size(); idx++){
        ArrayList<Double> test_x = new ArrayList<>();
        for (int j = 0; j < dim-1; j++) {
          test_x.add(valueArrayList.get(j).get(candidates.get(idx).get(j)));
        }
        double test_y = valueArrayList.get(dim-1).get(candidates.get(idx).get(dim-1));
        double predicted_y = model.predict(test_x);
        if (Math.abs(test_y - predicted_y) > delta) {
          candidates.remove(idx);
          idx --;
        }
      }
    }

    // alignment search
    HashMap<Integer, ArrayList<Integer>>[] idx_dict = new HashMap[dim]; // indexes are in how many tuples
    HashMap<Integer, HashSet<Integer>> intersect_dict = new HashMap<>();
    for (int i = 0; i < dim; i++) {
      idx_dict[i] = new HashMap<>();
    }
    for (int idx = 0; idx < candidates.size(); idx++) {
      for (int x = 0; x < dim; x++) {
        int value = candidates.get(idx).get(x);
        if (!idx_dict[x].containsKey(value)) {
          idx_dict[x].put(value, new ArrayList<>());
        }
        idx_dict[x].get(value).add(idx);
      }
    }
    for (int x = 0; x < dim; x++){
      HashMap<Integer, ArrayList<Integer>> dim_dict = idx_dict[x];
      for (Map.Entry entry : dim_dict.entrySet()) {
        int key = (int) entry.getKey();
        ArrayList<Integer> values = (ArrayList<Integer>) entry.getValue();
        if (dim_dict.get(key).size() >= 2) {
          for (int i = 0; i < values.size(); i++) {
            for (int j = i + 1; j < values.size(); j++) {
              int c1 = values.get(i);
              int c2 = values.get(j);
              if (!intersect_dict.containsKey(c1)) {
                intersect_dict.put(c1, new HashSet<>());
              }
              if (!intersect_dict.containsKey(c2)) {
                intersect_dict.put(c2, new HashSet<>());
              }
              intersect_dict.get(c1).add(c2);
              intersect_dict.get(c2).add(c1);
            }
          }
        }
      }
    }

    // greedy init
    ArrayList<Integer> R_sm = new ArrayList<>();
    HashSet<Integer> dislike_set = new HashSet<>();

    for (int i = 0; i < candidates.size(); i++) {
      if (!dislike_set.contains(i)) {
        R_sm.add(i);
        if (intersect_dict.containsKey(i)) {
          dislike_set.addAll(intersect_dict.get(i));
        }
      }
    }

    // perform alignment search algorithm
    boolean optimal = false;

    while (!optimal) {
      optimal = true;
      for (int q = 1; q < rho; q++) {
        ArrayList<ArrayList<Integer>> R_overlap_list = new ArrayList<>();
        for (Iterator<int[]> iter = new Combinations(R_sm.size(), q).iterator(); iter.hasNext();) {
          // init R_overlap
          int[] R_overlap_idx = iter.next();
          ArrayList<Integer> R_overlap = new ArrayList<>();
          for (int i = 0; i < R_overlap_idx.length; i++) {
            R_overlap.add(R_sm.get(R_overlap_idx[i]));
          }
          // pruning
          ArrayList<ArrayList<Integer>> selected_candidates = new ArrayList<>();
          for (int idx: R_overlap) {
            selected_candidates.add(candidates.get(idx));
            if (q > 1 && (!checkMinMaxTime(q, selected_candidates))) {
              continue;
            }
          }
          HashSet<Integer> joint_set = new HashSet<>();
          for (int r: R_overlap) {
            joint_set.addAll(intersect_dict.get(r));
          }

          // check
          if (joint_set.size() > q) {
            HashSet<Integer> R_replace = new HashSet<>();
            for (int r: joint_set) {
              HashSet<Integer> intersection = (HashSet<Integer>)intersect_dict.get(r).clone();
              intersection.retainAll(R_sm);
              HashSet<Integer> intersection2 = (HashSet<Integer>)intersect_dict.get(r).clone();
              intersection2.retainAll(R_replace);
              if (R_overlap.contains(intersection) && intersection2.isEmpty()) {
                R_replace.add(r);
              }
              if (R_replace.size() > q) {
                // conduct swapping
                R_sm.removeAll(R_overlap);
                R_sm.addAll(R_replace);
                optimal = false;
                break;
              }
            }
          }
        }
      }
    }
    // generate results according to the R_sm
    resultTimeList.clear();
    resultValueList.clear();
    Collections.sort(R_sm);
    ArrayList<ArrayList<Integer>> selected_candidates_indexes = new ArrayList<>();
    for (int r : R_sm) {
      selected_candidates_indexes.add(candidates.get(r));
    }
    for (ArrayList<Integer> candidate: selected_candidates_indexes) {
      resultTimeList.add(timeArrayList.get(0).get(candidate.get(0)));
      String resStr = "";
      int x = 0;
      for (; x < dim - 1; x++) {
        resStr += String.valueOf(valueArrayList.get(x).get(candidate.get(x)));
        resStr += ",";
      }
      resStr += String.valueOf(valueArrayList.get(x).get(candidate.get(x)));
      resultValueList.add(resStr);
    }
  }

  private boolean checkMinMaxTime(int q, ArrayList<ArrayList<Integer>> selected_candidates) {
    // prune
    long min_time = Long.MAX_VALUE;
    long max_time = 0;
    for (ArrayList<Integer> candidate: selected_candidates) {
      for (int x = 0; x < dim; x++) {
        int idx = candidate.get(x);
        long ts = this.timeArrayList.get(x).get(idx);
        min_time = Math.min(min_time, ts);
        max_time = Math.max(max_time, ts);
      }
    }
    return (max_time - min_time) < (2*q + 1) * theta;
  }

  public ArrayList<Long> getResultTimeList() {
    return resultTimeList;
  }

  public ArrayList<String> getResultValueList() {
      return resultValueList;
  }

  public void trainModel() {
    ArrayList<ArrayList<Double>> train_x = new ArrayList<>();
    ArrayList<Double> train_y = new ArrayList<>();
    for (int i = 0; i < resultValueList.size(); i++) {
      // a row
      ArrayList<Double> train_x_row = new ArrayList<>();
      String values = resultValueList.get(i);
      String[] value_list = values.split(",");
      for (int j = 0; j < dim-1; j++){
        train_x_row.add(Double.valueOf(value_list[j]));
      }
      train_x.add(train_x_row);
      train_y.add(Double.valueOf(value_list[dim-1]));
    }
    this.model.train(train_x, this.valueArrayList.get(dim-1));
  }

  class Model {
    // multipleLinearRegression model
    OLSMultipleLinearRegression model;
    boolean enable;
    public Model() {
      model = new OLSMultipleLinearRegression();
      model.setNoIntercept(true);
      enable = false;
    }
    public void train(ArrayList<ArrayList<Double>> train_x, ArrayList<Double> train_y) {
      double[][] sample_data_x = new double[train_x.size()][train_x.get(0).size()];
      double[] sample_data_y = new double[train_y.size()];
      for (int i = 0; i < train_x.size(); i++) {
        for (int j = 0; j < train_x.get(i).size(); j++) {
          sample_data_x[i][j] = train_x.get(i).get(j);
        }
        sample_data_y[i] = train_y.get(i);
      }
      model.newSampleData(sample_data_y, sample_data_x);
      enable = true;
    }

    public double predict(ArrayList<Double> test_x) {
      double[] coe = model.estimateRegressionParameters();
      double result = coe[0];
      for (int i = 1; i < coe.length; i++) {
        result += coe[i] * test_x.get(i-1);
      }
      return result;
    }

    public ArrayList<Double> predictBatch(ArrayList<ArrayList<Double>> test_x) {
      double[] coe = model.estimateRegressionParameters();
      ArrayList<Double> result = new ArrayList<>();
      for (int i = 0; i < test_x.size(); i++) {
        double res_tmp = coe[0];
        for (int j = 1; j < coe.length; j++) {
          res_tmp += coe[i] * test_x.get(i).get(j-1);
        }
        result.add(res_tmp);
      }
      return result;
    }

  }

  public static void main(String[] args) throws Exception {
    String[] fileList = new String[]{"temp.csv", "temp2.csv", "temp3.csv"};
    AlignmentExecuter alignmentExecuter = new AlignmentExecuter(fileList);
    alignmentExecuter.alignmentSearch();
    alignmentExecuter.trainModel();
    alignmentExecuter.alignmentSearch();

  }

}
