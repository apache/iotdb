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
 * Legendre's orthonormal wavelet of six coefficients and the scales; normed, due to ||*||2 -
 * euclidean norm.
 *
 * @date 03.06.2010 22:04:35
 * @author Christian (graetz23@gmail.com)
 */
public class Legendre3 extends Wavelet {

  /**
   * Constructor setting up the orthonormal Legendre6 wavelet coeffs and the scales; normed, due to
   * ||*||2 - euclidean norm.
   *
   * @date 03.06.2010 22:04:36
   * @author Christian (graetz23@gmail.com)
   */
  public Legendre3() {

    _name = "Legendre 3"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 6; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength]; // can be done in static way also; faster?
    _scalingDeCom[0] = -63. / 128.; // h0
    _scalingDeCom[1] = -35. / 128.; // h1
    _scalingDeCom[2] = -30. / 128.; // h2
    _scalingDeCom[3] = -30. / 128.; // h3
    _scalingDeCom[4] = -35. / 128.; // h4
    _scalingDeCom[5] = -63. / 128.; // h5

    // normalize orthogonal space => orthonormal space!!!
    double sqrt02 = Math.sqrt(2.); // 1.4142135623730951
    for (int i = 0; i < _motherWavelength; i++) _scalingDeCom[i] /= sqrt02;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Legendre3
} // class
