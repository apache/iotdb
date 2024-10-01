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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.legendre;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Orthonormal Legendre wavelet transform of 2 coefficients based on the Legendre polynomial. But,
 * actually for the smallest Legendre wavelet, the wavelet is the mirrored Haar Wavelet.
 *
 * @date 08.06.2010 09:32:08
 * @author Christian (graetz23@gmail.com)
 */
public class Legendre1 extends Wavelet {

  /**
   * Constructor setting up the orthonormal Legendre 2 wavelet coeffs and the scales; normed, due to
   * ||*||_2 -- euclidean norm. Actually these coefficients are the mirrored ones of Alfred Haar's
   * wavelet -- see class Haar1 and Haar1Orthogonal.
   *
   * @date 08.06.2010 09:32:08
   * @author Christian (graetz23@gmail.com)
   */
  public Legendre1() {

    _name = "Legendre 1"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 2; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -1.; // h0
    _scalingDeCom[1] = -1.; // h1

    // normalize orthogonal space => orthonormal space!!!
    double sqrt02 = Math.sqrt(2.); // 1.4142135623730951
    for (int i = 0; i < _motherWavelength; i++) _scalingDeCom[i] /= sqrt02;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Legendre1
} // class
