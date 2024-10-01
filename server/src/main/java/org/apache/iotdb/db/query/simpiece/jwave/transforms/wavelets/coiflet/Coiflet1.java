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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.coiflet;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Ingrid Daubechies' orthonormal Coiflet wavelet of six coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 15.02.2014 22:27:55
 */
public class Coiflet1 extends Wavelet {

  /**
   * Constructor calculating analytically the orthogonal Coiflet wavelet of 6 coefficients,
   * orthonormalizes them (normed, due to ||*||2 - euclidean norm), and spreads the scaling
   * coefficients afterwards; .
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 22:27:55
   */
  public Coiflet1() {

    _name = "Coiflet 1"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 6; // wavelength of mother wavelet

    double sqrt02 = 1.4142135623730951;
    double sqrt15 = Math.sqrt(15.);

    // these coefficients are already orthonormal
    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = sqrt02 * (sqrt15 - 3.) / 32.; //  -0.01565572813546454;
    _scalingDeCom[1] = sqrt02 * (1. - sqrt15) / 32.; // -0.0727326195128539;
    _scalingDeCom[2] = sqrt02 * (6. - 2 * sqrt15) / 32.; //  0.38486484686420286;
    _scalingDeCom[3] = sqrt02 * (2. * sqrt15 + 6.) / 32.; // 0.8525720202122554;
    _scalingDeCom[4] = sqrt02 * (sqrt15 + 13.) / 32.; // 0.3378976624578092;
    _scalingDeCom[5] = sqrt02 * (9. - sqrt15) / 32.; // -0.0727326195128539;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Coiflet1
} // class
