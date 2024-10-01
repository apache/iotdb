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
 * Ingrid Daubechies' orthonormal Coiflet wavelet of 18 coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 15.02.2014 22:58:59
 */
public class Coiflet3 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/coif3/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 22:58:59
   */
  public Coiflet3() {

    _name = "Coiflet 3"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 18; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -3.459977283621256e-05;
    _scalingDeCom[1] = -7.098330313814125e-05;
    _scalingDeCom[2] = 0.0004662169601128863;
    _scalingDeCom[3] = 0.0011175187708906016;
    _scalingDeCom[4] = -0.0025745176887502236;
    _scalingDeCom[5] = -0.00900797613666158;
    _scalingDeCom[6] = 0.015880544863615904;
    _scalingDeCom[7] = 0.03455502757306163;
    _scalingDeCom[8] = -0.08230192710688598;
    _scalingDeCom[9] = -0.07179982161931202;
    _scalingDeCom[10] = 0.42848347637761874;
    _scalingDeCom[11] = 0.7937772226256206;
    _scalingDeCom[12] = 0.4051769024096169;
    _scalingDeCom[13] = -0.06112339000267287;
    _scalingDeCom[14] = -0.0657719112818555;
    _scalingDeCom[15] = 0.023452696141836267;
    _scalingDeCom[16] = 0.007782596427325418;
    _scalingDeCom[17] = -0.003793512864491014;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Coiflet3
} // Coiflet3
