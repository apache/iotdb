package org.apache.iotdb.library.anomaly.util;

import java.util.ArrayList;

public class VAR {
  private final int p;
  private ArrayList<ArrayList<Double>> coeffs;

  public VAR(int p) {
    this.p = p;
    this.coeffs = new ArrayList<>();
  }

  // Train the model with data to get coefficients
  public void fit(ArrayList<ArrayList<Double>> data) {
    int n = data.size();
    int k = data.get(0).size();

    ArrayList<ArrayList<Double>> X = new ArrayList<>();

    // Construct the data matrix X
    for (int i = p; i < n; i++) {
      ArrayList<Double> x = new ArrayList<>();
      for (int j = 0; j < p; j++) {
        x.addAll(data.get(i - j - 1));
      }
      X.add(x);
    }

    // Compute the coefficients using OLS
    Matrix Xmat = new Matrix(X);
    ArrayList<ArrayList<Double>> Yarry = new ArrayList<>();
    for (int i = p; i < n; i++) {
      Yarry.add(data.get(i));
    }
    Matrix Ymat = new Matrix(Yarry);
    Matrix XtX = Xmat.transpose().multiply(Xmat);
    Matrix XtY = Xmat.transpose().multiply(Ymat);
    Matrix beta = XtX.solve(XtY);
    this.coeffs = beta.transpose().getData();
  }

  public void fitCoeffs(ArrayList<Double> coeffs_in_one_column, int column_cnt) {
    ArrayList<ArrayList<Double>> coeffs = new ArrayList<>();
    int single_size = coeffs_in_one_column.size() / column_cnt;
    for (int i = 0; i < coeffs_in_one_column.size(); i += single_size) {
      ArrayList<Double> temp = new ArrayList<>();
      for (int j = 0; j < single_size; j++) {
        temp.add(coeffs_in_one_column.get(i + j));
      }
      coeffs.add(temp);
    }
    this.coeffs = coeffs;
  }

  // One step of prediction based on window. Window has p tuples.
  // Return the prediction result.
  public ArrayList<Double> predict(ArrayList<ArrayList<Double>> window) {
    int k = window.get(0).size();
    ArrayList<Double> x = new ArrayList<>();
    for (int i = 0; i < p; i++) {
      x.addAll(window.get(p - i - 1));
    }
    double[] yhat = new double[k];
    ArrayList<Double> prediction_tuple = new ArrayList<>();
    for (int j = 0; j < k; j++) {
      for (int i = 0; i < x.size(); i++) {
        yhat[j] += x.get(i) * coeffs.get(j).get(i);
      }
      prediction_tuple.add(yhat[j]);
    }

    return prediction_tuple;
  }

  public ArrayList<ArrayList<Double>> getCoeffs() {
    return coeffs;
  }

  // Helper class for matrix operations
  public class Matrix {
    public final int m;
    public final int n;
    public final ArrayList<ArrayList<Double>> data;

    public Matrix(int m, int n) {
      this.m = m;
      this.n = n;
      this.data = new ArrayList<ArrayList<Double>>(m);
      for (int i = 0; i < m; i++) {
        ArrayList<Double> row = new ArrayList<Double>(n);
        for (int j = 0; j < n; j++) {
          row.add(0.0);
        }
        this.data.add(row);
      }
    }

    public Matrix(ArrayList<ArrayList<Double>> data) {
      this.m = data.size();
      this.n = data.get(0).size();
      this.data = new ArrayList<ArrayList<Double>>(m);
      for (int i = 0; i < m; i++) {
        ArrayList<Double> row = new ArrayList<Double>(n);
        for (int j = 0; j < n; j++) {
          row.add(data.get(i).get(j));
        }
        this.data.add(row);
      }
    }

    public Matrix transpose() {
      Matrix A = this;
      Matrix C = new Matrix(n, m);
      for (int i = 0; i < m; i++) {
        for (int j = 0; j < n; j++) {
          C.data.get(j).set(i, A.data.get(i).get(j));
        }
      }
      return C;
    }

    public Matrix multiply(Matrix B) {
      Matrix A = this;
      if (A.n != B.m) {
        throw new IllegalArgumentException("Matrix dimensions don't match");
      }
      Matrix C = new Matrix(A.m, B.n);
      for (int i = 0; i < C.m; i++) {
        for (int j = 0; j < C.n; j++) {
          for (int k = 0; k < A.n; k++) {
            C.data
                .get(i)
                .set(j, C.data.get(i).get(j) + A.data.get(i).get(k) * B.data.get(k).get(j));
          }
        }
      }
      return C;
    }

    public ArrayList<ArrayList<Double>> getArray() {
      return data;
    }

    public Matrix solve(Matrix B) {
      Matrix A = this;
      if (A.m != A.n || A.m != B.m) {
        throw new IllegalArgumentException("Matrix dimensions don't match");
      }

      int n = A.n;
      Matrix[] LU = A.lu();
      Matrix L = LU[0];
      Matrix U = LU[1];

      // Solve LY = B using forward substitution
      Matrix Y = new Matrix(n, B.n);
      for (int j = 0; j < B.n; j++) {
        for (int i = 0; i < n; i++) {
          Y.data.get(i).set(j, B.data.get(i).get(j));
          for (int k = 0; k < i; k++) {
            Y.data
                .get(i)
                .set(j, Y.data.get(i).get(j) - L.data.get(i).get(k) * Y.data.get(k).get(j));
          }
        }
      }

      // Solve UX = Y using backward substitution
      Matrix X = new Matrix(n, B.n);
      for (int j = 0; j < B.n; j++) {
        for (int i = n - 1; i >= 0; i--) {
          X.data.get(i).set(j, Y.data.get(i).get(j));
          for (int k = i + 1; k < n; k++) {
            X.data
                .get(i)
                .set(j, X.data.get(i).get(j) - U.data.get(i).get(k) * X.data.get(k).get(j));
          }
          X.data.get(i).set(j, X.data.get(i).get(j) / U.data.get(i).get(i));
        }
      }
      return X;
    }

    public Matrix[] lu() {
      Matrix A = this;
      if (A.m != A.n) {
        throw new IllegalArgumentException("Matrix dimensions don't match");
      }

      Matrix L = new Matrix(A.m, A.n);
      Matrix U = new Matrix(A.m, A.n);
      for (int j = 0; j < A.n; j++) {
        L.data.get(j).set(j, 1.0);
        for (int i = 0; i < j + 1; i++) {
          double s1 = 0.0;
          for (int k = 0; k < i; k++) {
            s1 += U.data.get(k).get(j) * L.data.get(i).get(k);
          }
          U.data.get(i).set(j, A.data.get(i).get(j) - s1);
        }
        for (int i = j + 1; i < A.n; i++) {
          double s2 = 0.0;
          for (int k = 0; k < j; k++) {
            s2 += U.data.get(k).get(j) * L.data.get(i).get(k);
          }
          L.data.get(i).set(j, (A.data.get(i).get(j) - s2) / U.data.get(j).get(j));
        }
      }
      return new Matrix[] {L, U};
    }

    public ArrayList<ArrayList<Double>> getData() {
      return data;
    }
  }
}
