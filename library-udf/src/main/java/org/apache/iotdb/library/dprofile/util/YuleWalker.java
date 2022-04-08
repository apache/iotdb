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
package org.apache.iotdb.library.dprofile.util;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularMatrixException;

import java.util.Arrays;

/** Describe class here. */
public class YuleWalker {
  public double yuleWalker(double[] x, int order, String method, int n) {
    double adj_needed = method.equalsIgnoreCase("adjusted") ? 1 : 0;
    double[] r = new double[order + 1];
    double squaresumx = 0d;
    for (double v : x) {
      squaresumx += v * v;
    }
    r[0] = squaresumx / n;
    for (int k = 1; k < order + 1; k++) {
      double[] t1 = Arrays.copyOfRange(x, 0, n - k);
      double[] t2 = Arrays.copyOfRange(x, k, n);
      double crossmultiplysum = 0d;
      for (int i = 0; i < Math.min(t1.length, t2.length); i++) {
        crossmultiplysum += t1[i] * t2[i];
      }
      r[k] = crossmultiplysum / (n - k * adj_needed);
    }
    // R is a toeplitz matrix
    double[][] R = new double[r.length - 1][r.length - 1];
    for (int i = 0; i < r.length - 1; i++) {
      for (int j = 0; j < r.length - 1; j++) {
        R[i][j] = r[Math.abs(i - j)];
      }
    }
    RealMatrix a = new Array2DRowRealMatrix(R, true);
    RealVector b = new ArrayRealVector(Arrays.copyOfRange(r, 1, r.length), true);
    DecompositionSolver solver = new LUDecomposition(a).getSolver();
    try {
      RealVector rho = solver.solve(b);
      /*
      sigmasq = r[0] - (r[1:]*rho).sum()
      sigma = np.sqrt(sigmasq) if not np.isnan(sigmasq) and sigmasq > 0 else np.nan
      */
      return rho.getEntry(rho.getDimension() - 1);
    } catch (SingularMatrixException e) {
      return Double.NaN;
    }
  }
}
