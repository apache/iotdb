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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.other;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Cohen Daubechies Feauveau (CDF) 9/7 wavelet. THIS WAVELET IS NOT WORKING - DUE TO ODD NUMBER
 * COEFFICIENTS!!!
 *
 * @date 17.08.2014 08:41:55
 * @author Christian (graetz23@gmail.com)
 */
public class CDF53 extends Wavelet {

  /**
   * THIS WAVELET IS NOT WORKING - DUE TO ODD NUMBER COEFFICIENTS!!!
   *
   * @date 17.08.2014 08:41:55
   * @author Christian (graetz23@gmail.com)
   */
  @Deprecated
  public CDF53() {

    _name = "CDF 5/3"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 5; // wavelength of mother wavelet

    //    double sqrt2 = Math.sqrt( 2. );

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -1. / 8.; // - 1/8
    _scalingDeCom[1] = 1. / 4.; // + 2/8
    _scalingDeCom[2] = 3. / 4.; // + 6/8
    _scalingDeCom[3] = 1. / 4.; // + 2/8
    _scalingDeCom[4] = -1. / 8.; // - 1/8

    _waveletDeCom = new double[_motherWavelength];
    _waveletDeCom[0] = 0; //
    _waveletDeCom[1] = 1. / 2.; //
    _waveletDeCom[2] = 1.; //
    _waveletDeCom[3] = 1. / 2.; //
    _waveletDeCom[4] = 0; //

    // Copy to reconstruction filters due to orthogonality!
    _scalingReCon = new double[_motherWavelength];
    _waveletReCon = new double[_motherWavelength];
    for (int i = 0; i < _motherWavelength; i++) {
      _scalingReCon[i] = _scalingDeCom[i];
      _waveletReCon[i] = _waveletDeCom[i];
    } // i
  } // CDF53
} // class
