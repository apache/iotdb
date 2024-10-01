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
 * Ingrid Daubechies' orthonormal wavelet of eight coefficients and the scales; normed, due to
 * ||*||2 - euclidean norm.
 *
 * @date 26.03.2010 07:35:31
 * @author Christian (graetz23@gmail.com)
 */
public class Daubechies4 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db4/ Thanks!
   *
   * @date 26.03.2010 07:35:31
   * @author Christian (graetz23@gmail.com)
   */
  public Daubechies4() {

    _name = "Daubechies 4"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 8; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -0.010597401784997278;
    _scalingDeCom[1] = 0.032883011666982945;
    _scalingDeCom[2] = 0.030841381835986965;
    _scalingDeCom[3] = -0.18703481171888114;
    _scalingDeCom[4] = -0.02798376941698385;
    _scalingDeCom[5] = 0.6308807679295904;
    _scalingDeCom[6] = 0.7148465705525415;
    _scalingDeCom[7] = 0.23037781330885523;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies4
} // class
