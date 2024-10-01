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
 * Ingrid Daubechies' orthonormal Daubechies wavelet of 36 coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 19.08.2014 18:10:59
 */
public class Daubechies18 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db18/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:10:59
   */
  public Daubechies18() {

    _name = "Daubechies 18"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 36; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = -2.507934454941929e-09;
    _scalingDeCom[1] = 3.06883586303703e-08;
    _scalingDeCom[2] = -1.1760987670250871e-07;
    _scalingDeCom[3] = -7.691632689865049e-08;
    _scalingDeCom[4] = 1.768712983622886e-06;
    _scalingDeCom[5] = -3.3326344788769603e-06;
    _scalingDeCom[6] = -8.520602537423464e-06;
    _scalingDeCom[7] = 3.741237880730847e-05;
    _scalingDeCom[8] = -1.535917123021341e-07;
    _scalingDeCom[9] = -0.00019864855231101547;
    _scalingDeCom[10] = 0.0002135815619103188;
    _scalingDeCom[11] = 0.0006284656829644715;
    _scalingDeCom[12] = -0.0013405962983313922;
    _scalingDeCom[13] = -0.0011187326669886426;
    _scalingDeCom[14] = 0.004943343605456594;
    _scalingDeCom[15] = 0.00011863003387493042;
    _scalingDeCom[16] = -0.013051480946517112;
    _scalingDeCom[17] = 0.006262167954438661;
    _scalingDeCom[18] = 0.026670705926689853;
    _scalingDeCom[19] = -0.023733210395336858;
    _scalingDeCom[20] = -0.04452614190225633;
    _scalingDeCom[21] = 0.05705124773905827;
    _scalingDeCom[22] = 0.0648872162123582;
    _scalingDeCom[23] = -0.10675224665906288;
    _scalingDeCom[24] = -0.09233188415030412;
    _scalingDeCom[25] = 0.16708131276294505;
    _scalingDeCom[26] = 0.14953397556500755;
    _scalingDeCom[27] = -0.21648093400458224;
    _scalingDeCom[28] = -0.2936540407357981;
    _scalingDeCom[29] = 0.14722311196952223;
    _scalingDeCom[30] = 0.571801654887122;
    _scalingDeCom[31] = 0.5718268077650818;
    _scalingDeCom[32] = 0.31467894133619284;
    _scalingDeCom[33] = 0.10358846582214751;
    _scalingDeCom[34] = 0.01928853172409497;
    _scalingDeCom[35] = 0.0015763102184365595;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies18
} // Daubechies18
