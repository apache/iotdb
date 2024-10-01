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
 * Symlet10 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 19.08.2014 18:23:14
 */
public class Symlet14 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym14/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:23:14
   */
  public Symlet14() {

    _name = "Symlet 14"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 28; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = -2.5879090265397886e-05;
    _scalingDeCom[1] = 1.1210865808890361e-05;
    _scalingDeCom[2] = 0.00039843567297594335;
    _scalingDeCom[3] = -6.286542481477636e-05;
    _scalingDeCom[4] = -0.002579441725933078;
    _scalingDeCom[5] = 0.0003664765736601183;
    _scalingDeCom[6] = 0.01003769371767227;
    _scalingDeCom[7] = -0.002753774791224071;
    _scalingDeCom[8] = -0.029196217764038187;
    _scalingDeCom[9] = 0.004280520499019378;
    _scalingDeCom[10] = 0.03743308836285345;
    _scalingDeCom[11] = -0.057634498351326995;
    _scalingDeCom[12] = -0.03531811211497973;
    _scalingDeCom[13] = 0.39320152196208885;
    _scalingDeCom[14] = 0.7599762419610909;
    _scalingDeCom[15] = 0.4753357626342066;
    _scalingDeCom[16] = -0.05811182331771783;
    _scalingDeCom[17] = -0.15999741114652205;
    _scalingDeCom[18] = 0.02589858753104667;
    _scalingDeCom[19] = 0.06982761636180755;
    _scalingDeCom[20] = -0.002365048836740385;
    _scalingDeCom[21] = -0.019439314263626713;
    _scalingDeCom[22] = 0.0010131419871842082;
    _scalingDeCom[23] = 0.004532677471945648;
    _scalingDeCom[24] = -7.321421356702399e-05;
    _scalingDeCom[25] = -0.0006057601824664335;
    _scalingDeCom[26] = 1.9329016965523917e-05;
    _scalingDeCom[27] = 4.4618977991475265e-05;

    _buildOrthonormalSpace();
  } // Symlet14
} // Symlet14
