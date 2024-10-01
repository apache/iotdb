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
 * Ingrid Daubechies' orthonormal wavelet of 18 coefficients and the scales; normed, due to ||*||2 -
 * euclidean norm.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 00:34:30
 */
public class Daubechies9 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db9/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 00:34:30
   */
  public Daubechies9() {

    _name = "Daubechies 9"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 18; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 3.9347319995026124e-05;
    _scalingDeCom[1] = -0.0002519631889981789;
    _scalingDeCom[2] = 0.00023038576399541288;
    _scalingDeCom[3] = 0.0018476468829611268;
    _scalingDeCom[4] = -0.004281503681904723;
    _scalingDeCom[5] = -0.004723204757894831;
    _scalingDeCom[6] = 0.022361662123515244;
    _scalingDeCom[7] = 0.00025094711499193845;
    _scalingDeCom[8] = -0.06763282905952399;
    _scalingDeCom[9] = 0.030725681478322865;
    _scalingDeCom[10] = 0.14854074933476008;
    _scalingDeCom[11] = -0.09684078322087904;
    _scalingDeCom[12] = -0.29327378327258685;
    _scalingDeCom[13] = 0.13319738582208895;
    _scalingDeCom[14] = 0.6572880780366389;
    _scalingDeCom[15] = 0.6048231236767786;
    _scalingDeCom[16] = 0.24383467463766728;
    _scalingDeCom[17] = 0.03807794736316728;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies9
} // Daubechies9
