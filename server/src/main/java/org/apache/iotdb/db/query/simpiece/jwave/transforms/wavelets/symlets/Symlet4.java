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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Symlet4 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 17.08.2014 14:11:10
 */
public class Symlet4 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym4/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 14:11:10
   */
  public Symlet4() {

    _name = "Symlet 4"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 8; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -0.07576571478927333;
    _scalingDeCom[1] = -0.02963552764599851;
    _scalingDeCom[2] = 0.49761866763201545;
    _scalingDeCom[3] = 0.8037387518059161;
    _scalingDeCom[4] = 0.29785779560527736;
    _scalingDeCom[5] = -0.09921954357684722;
    _scalingDeCom[6] = -0.012603967262037833;
    _scalingDeCom[7] = 0.0322231006040427;

    _buildOrthonormalSpace();
  } // Symlet4
} // Symlet4
