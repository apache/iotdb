/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.library.frequency.util;

import java.util.ArrayList;
import java.util.Arrays;

/** Util for UDTFDWT and UDTFIDWT */
public class DWTUtil {
  /** The number of coefficients. */
  private int ncof;
  /** layers to decompose，When transforming, point numbers should be no less than ncof */
  private int layer;
  /** Centering. */
  private int ioff;

  private int joff;

  /** Wavelet coefficients. */
  private double[] cc;

  private double[] cr;

  /** Data storage */
  private double[] data;

  /** Workspace. */
  private double[] workspace = new double[1024];

  public DWTUtil(String method, String coef, int layer, ArrayList<Double> data) {
    this.data = data.stream().mapToDouble(Double::valueOf).toArray();
    this.layer = layer;
    if (method.equalsIgnoreCase("Haar") || method.equalsIgnoreCase("DB2")) {
      cc = new double[] {1 / Math.sqrt(2), 1 / Math.sqrt(2)};
    } else if (method.equalsIgnoreCase("DB4")) {
      cc =
          new double[] {
            0.4829629131445341, 0.8365163037378077, 0.2241438680420134, -0.1294095225512603
          };
    } else if (method.equalsIgnoreCase("DB6")) {
      cc =
          new double[] {
            0.3326705529500825,
            0.8068915093110924,
            0.4598775021184914,
            -0.1350110200102546,
            -0.0854412738820267,
            0.0352262918857095
          };
    } else if (method.equalsIgnoreCase("DB8")) {
      cc =
          new double[] {
            0.2303778133088964,
            0.7148465705529154,
            0.6308807679398587,
            -0.0279837694168599,
            -0.1870348117190931,
            0.0308413818355607,
            0.0328830116668852,
            -0.0105974017850690
          };
    } else {
      String[] coefString = coef.split(",");
      ncof = coefString.length;
      cc = new double[ncof];
      for (int i = 0; i < ncof; i++) {
        cc[i] = Double.parseDouble(coefString[i]);
      }
    }
    ncof = cc.length;
    ioff = joff = -(ncof >> 1);
    cr = new double[ncof];
    double sig = -1.0;
    for (int i = 0; i < ncof; i++) {
      cr[ncof - 1 - i] = sig * cc[i];
      sig = -sig;
    }
  }

  public static boolean isPower2(int x) {
    return x > 0 && (x & (x - 1)) == 0;
  }
  /**
   * Log of base 2.
   *
   * @param x a real number.
   * @return the value <code>log2(x)</code>.
   */
  public static double log2(double x) {
    return Math.log(x) / Math.log(2);
  }

  /**
   * Applies the wavelet filter to a signal vector a[0, n-1].
   *
   * @param n the length of vector.
   */
  void forward(int n) {
    if (n < ncof) {
      return;
    }

    if (n > workspace.length) {
      workspace = new double[n];
    } else {
      Arrays.fill(workspace, 0, n, 0.0);
    }

    int nmod = ncof * n;
    int n1 = n - 1;
    int nh = n >> 1;

    for (int ii = 0, i = 0; i < n; i += 2, ii++) {
      int ni = i + 1 + nmod + ioff;
      int nj = i + 1 + nmod + joff;
      for (int k = 0; k < ncof; k++) {
        int jf = n1 & (ni + k);
        int jr = n1 & (nj + k);
        workspace[ii] += cc[k] * data[jf];
        workspace[ii + nh] += cr[k] * data[jr];
      }
    }

    System.arraycopy(workspace, 0, data, 0, n);
  }

  /** Discrete wavelet transform. */
  public void waveletTransform() {
    int n = data.length;

    if (!isPower2(n)) {
      throw new IllegalArgumentException("The data vector size is not a power of 2.");
    }

    if (n < ncof) {
      throw new IllegalArgumentException(
          "The data vector size is less than wavelet coefficient size.");
    }
    int nn = n;
    for (int i = 0; i < layer; i++) {
      if (nn < ncof) {
        break;
      }
      forward(nn);
      n >>= 1;
    }
  }

  void backward(int n) {
    if (n < ncof) {
      return;
    }

    if (n > workspace.length) {
      workspace = new double[n];
    } else {
      Arrays.fill(workspace, 0, n, 0.0);
    }

    int nmod = ncof * n;
    int n1 = n - 1;
    int nh = n >> 1;

    for (int ii = 0, i = 0; i < n; i += 2, ii++) {
      double ai = data[ii];
      double ai1 = data[ii + nh];
      int ni = i + 1 + nmod + ioff;
      int nj = i + 1 + nmod + joff;
      for (int k = 0; k < ncof; k++) {
        int jf = n1 & (ni + k);
        int jr = n1 & (nj + k);
        workspace[jf] += cc[k] * ai;
        workspace[jr] += cr[k] * ai1;
      }
    }

    System.arraycopy(workspace, 0, data, 0, n);
  }

  /**
   * Inverse discrete wavelet transform. Input series has been transformed to the highest possible
   * layer, which means there is 1 number in the last layer. To alter this, you may refer to
   * waveletTransform()
   */
  public void inverse() {
    int n = data.length;

    if (!isPower2(n)) {
      throw new IllegalArgumentException("The data vector size is not a power of 2.");
    }

    if (n < ncof) {
      throw new IllegalArgumentException(
          "The data vector size is less than wavelet coefficient size.");
    }
    int nn = n;
    for (int i = 0; i < layer - 1; i++) {
      nn = n / 2;
    }
    for (int i = 0; i < layer; i++) {
      if (nn > n) {
        break;
      }
      backward(nn);
      n <<= 1;
    }
  }

  public double[] getData() {
    return data;
  }
}
