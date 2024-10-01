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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Ingrid Daubechies' orthonormal wavelet of six coefficients and the scales; normed, due to ||*||2
 * - euclidean norm.
 *
 * @date 15.02.2014 22:23:20
 * @author Christian (graetz23@gmail.com)
 */
public class Daubechies3 extends Wavelet {

  /**
   * Constructor setting up the orthonormal Daubechie6 wavelet coeffs and the scales; normed, due to
   * ||*||2 - euclidean norm.
   *
   * @date 25.03.2010 14:03:20
   * @author Christian (graetz23@gmail.com)
   */
  public Daubechies3() {

    _name = "Daubechies 3"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 6; // wavelength of mother wavelet

    // calculate the coefficients analytically
    _scalingDeCom = new double[_motherWavelength]; // can be done in static way also; faster?
    double sqrt10 = Math.sqrt(10.);
    double constA = Math.sqrt(5. + 2. * sqrt10);
    _scalingDeCom[0] = (1.0 + 1. * sqrt10 + 1. * constA) / 16.; // h0 = 0.47046720778416373
    _scalingDeCom[1] = (5.0 + 1. * sqrt10 + 3. * constA) / 16.; // h1 = 1.1411169158314438
    _scalingDeCom[2] = (10. - 2. * sqrt10 + 2. * constA) / 16.; // h2 = 0.6503650005262325
    _scalingDeCom[3] = (10. - 2. * sqrt10 - 2. * constA) / 16.; // h3 = -0.1909344155683274
    _scalingDeCom[4] = (5.0 + 1. * sqrt10 - 3. * constA) / 16.; // h4 = -0.1208322083103962
    _scalingDeCom[5] = (1.0 + 1. * sqrt10 - 1. * constA) / 16.; // h5 = 0.049817499736883764

    // normalize orthogonal space => orthonormal space!!!
    double sqrt02 = Math.sqrt(2.); // 1.4142135623730951
    for (int i = 0; i < _motherWavelength; i++) _scalingDeCom[i] /= sqrt02;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies3
} // class
