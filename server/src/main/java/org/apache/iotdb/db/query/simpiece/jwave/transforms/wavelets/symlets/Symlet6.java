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
 * Symlet6 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 17.08.2014 14:21:44
 */
public class Symlet6 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym6/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 14:21:44
   */
  public Symlet6() {

    _name = "Symlet 6"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 12; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.015404109327027373;
    _scalingDeCom[1] = 0.0034907120842174702;
    _scalingDeCom[2] = -0.11799011114819057;
    _scalingDeCom[3] = -0.048311742585633;
    _scalingDeCom[4] = 0.4910559419267466;
    _scalingDeCom[5] = 0.787641141030194;
    _scalingDeCom[6] = 0.3379294217276218;
    _scalingDeCom[7] = -0.07263752278646252;
    _scalingDeCom[8] = -0.021060292512300564;
    _scalingDeCom[9] = 0.04472490177066578;
    _scalingDeCom[10] = 0.0017677118642428036;
    _scalingDeCom[11] = -0.007800708325034148;

    _buildOrthonormalSpace();
  } // Symlet6
} // Symlet6
