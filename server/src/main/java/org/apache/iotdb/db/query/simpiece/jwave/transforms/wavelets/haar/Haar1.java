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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.haar;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Alfred Haar's orthonormal wavelet transform.
 *
 * @date 08.02.2010 12:46:34
 * @author Christian (graetz23@gmail.com)
 */
public class Haar1 extends Wavelet {

  /**
   * Constructor setting up the orthonormal Haar wavelet coefficients and the scaling coefficients;
   * normed, due to ||*||_2 -- euclidean norm. See the orthogonal version in class Haar1Orthogonal
   * for more details.
   *
   * @date 08.02.2010 12:46:34
   * @author Christian (graetz23@gmail.com)
   */
  public Haar1() {

    _name = "Haar"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 2; // wavelength of mother wavelet

    double sqrt2 = Math.sqrt(2.);

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 1. / sqrt2; // w0 = 1.4142135623730951 - normalized by square root of 2
    _scalingDeCom[1] = 1. / sqrt2; // w1 = 1.4142135623730951 - normalized by square root of 2

    _waveletDeCom = new double[_motherWavelength];
    _waveletDeCom[0] = _scalingDeCom[1]; // s0 = w1 - odd scaling is even wavelet parameter
    _waveletDeCom[1] =
        -_scalingDeCom[0]; // s1 = -w0 - even scaling is negative odd wavelet parameter

    // Copy to reconstruction filters due to orthogonality (orthonormality)!
    _scalingReCon = new double[_motherWavelength];
    _waveletReCon = new double[_motherWavelength];
    for (int i = 0; i < _motherWavelength; i++) {
      _scalingReCon[i] = _scalingDeCom[i]; //
      _waveletReCon[i] = _waveletDeCom[i];
    } // i
  } // Haar1

  /**
   * The forward wavelet transform using the Alfred Haar's wavelet.
   *
   * @date 10.02.2010 08:26:06
   * @author Christian (graetz23@gmail.com)
   * @see Wavelet#forward(double[])
   */

  /**
   * The reverse wavelet transform using the Alfred Haar's wavelet. The arrHilb array keeping
   * coefficients of Hilbert domain should be of length 2 to the power of p -- length = 2^p where p
   * is a positive integer.
   *
   * @date 10.02.2010 08:26:06
   * @author Christian (graetz23@gmail.com)
   * @see Wavelet#reverse(double[])
   */
} // class
