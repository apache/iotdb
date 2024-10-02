/**
 * JWave is distributed under the MIT License (MIT); this file is part of.
 *
 * <p>Copyright (c) 2008-2024 Christian (graetz23@gmail.com)
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.apache.iotdb.db.query.simpiece.jwave.transforms;

import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.tools.MathToolKit;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;

/**
 * A wavelet transform method for arrays and signals of arbitrary lengths, even odd lengths. The
 * array is decomposed in several parts of optimal lengths by applying the ancient Egyptian
 * decomposition. Hereby, the array or signal is decomposed to the largest possible sub arrays of
 * two the power of p. Afterwards each sub array is transformed forward and copied back to the
 * discrete position of the input array. The reverse transform applies the same vice versa. In more
 * detail the ancient Egyptian Multiplication can be easily explained by the following example: 42 =
 * 2^5 + 2^3 + 2^1 = 32 + 8 + 2. However, an array or signal of odd length produces the smallest
 * ancient Egyptian multiplier 2^0 which is actually 1. Therefore, the matching sub array or signal
 * is untouched an the coefficient is actually the wavelet coefficient of wavelet space of level 0.
 * For an "orthonormal" wavelet this holds. See:
 * http://en.wikipedia.org/wiki/Ancient_Egyptian_multiplication
 *
 * @author Christian (graetz23@gmail.com)
 * @date 14.08.2010 10:43:28
 */
public class AncientEgyptianDecomposition extends BasicTransform {

  /**
   * The selected Transform (FWT or WPT) used for the sub arrays of the ancient Egyptian
   * decomposition. Actually, this displays somehow the Composite Pattern of software design
   * pattern. See: http://en.wikipedia.org/wiki/Composite_pattern#Java
   */
  protected BasicTransform _basicTransform;

  /**
   * the base block size for spitting an array; e. g. 127 with block size of 32 ends up as: 32 | 32
   * | 32 | 16 | 8 | 4 | 2 | 1.
   */
  @SuppressWarnings("unused")
  private int _initialWaveletSpaceSize;

  /**
   * Constructor taking the
   *
   * @date 14.08.2010 10:43:28
   * @author Christian (graetz23@gmail.com)
   */
  public AncientEgyptianDecomposition(BasicTransform basicTransform) {

    _basicTransform = basicTransform;

    _initialWaveletSpaceSize = 0;
  } // FastBasicTransformArbitrary

  public AncientEgyptianDecomposition(BasicTransform waveTransform, int initialWaveletSpaceSize) {

    _basicTransform = waveTransform;

    _initialWaveletSpaceSize = initialWaveletSpaceSize;
  } // AncientEgyptianDecomposition

  /**
   * This forward method decomposes the given array of arbitrary length to sub arrays while applying
   * the ancient Egyptian decomposition. Each sub array is transformed by the selected basic
   * transform and the resulting wavelet coefficients are copied back to their original discrete
   * positions.
   *
   * @throws JWaveException
   * @date 14.08.2010 10:43:28
   * @author Christian (graetz23@gmail.com)
   * @see jwave.transforms.BasicTransform#forward(double[])
   */
  @Override
  public double[] forward(double[] arrTime) throws JWaveException {

    double[] arrHilb = new double[arrTime.length];

    int[] ancientEgyptianMultipliers = null;

    ancientEgyptianMultipliers = MathToolKit.decompose(arrTime.length);

    int offSet = 0;
    for (int m = 0; m < ancientEgyptianMultipliers.length; m++) {

      int ancientEgyptianMultiplier = ancientEgyptianMultipliers[m];

      int arrTimeSubLength = (int) MathToolKit.scalb(1., ancientEgyptianMultiplier);

      double[] arrTimeSub = new double[arrTimeSubLength];

      for (int i = 0; i < arrTimeSub.length; i++) {

        IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++; // note

        arrTimeSub[i] = arrTime[i + offSet];
      }

      double[] arrHilbSub = _basicTransform.forward(arrTimeSub);

      for (int i = 0; i < arrHilbSub.length; i++) {
        arrHilb[i + offSet] = arrHilbSub[i];
      }

      offSet += arrHilbSub.length;
    } // m - no of sub transforms

    return arrHilb;
  } // forward

  /**
   * This reverse method awaits an array of arbitrary length in wavelet space keeping the wavelet
   * already decomposed by the ancient Egyptian decomposition. Therefore, each of the existing sub
   * arrays of length 2^p is reverse transformed by the selected basic transform and the resulting
   * coefficients of time domain are copied back to their original discrete positions.
   *
   * @throws JWaveException
   * @date 14.08.2010 10:43:28
   * @author Christian (graetz23@gmail.com)
   * @see jwave.transforms.BasicTransform#reverse(double[])
   */
  @Override
  public double[] reverse(double[] arrHilb) throws JWaveException {

    double[] arrTime = new double[arrHilb.length];

    int[] ancientEgyptianMultipliers = null;
    try {

      ancientEgyptianMultipliers = MathToolKit.decompose(arrHilb.length);

    } catch (JWaveException e) {

      e.printStackTrace();
    }

    int offSet = 0;
    for (int m = 0; m < ancientEgyptianMultipliers.length; m++) {

      int ancientEgyptianMultiplier = ancientEgyptianMultipliers[m];

      int arrHilbSubLength = (int) MathToolKit.scalb(1., ancientEgyptianMultiplier);

      double[] arrHilbSub = new double[arrHilbSubLength];

      for (int i = 0; i < arrHilbSub.length; i++) {
        arrHilbSub[i] = arrHilb[i + offSet];
      }

      double[] arrTimeSub = _basicTransform.reverse(arrHilbSub);

      for (int i = 0; i < arrTimeSub.length; i++) {
        arrTime[i + offSet] = arrTimeSub[i];
      }

      offSet += arrHilbSub.length;
    } // m - no of sub transforms

    return arrTime;
  } // reverse
} // class
