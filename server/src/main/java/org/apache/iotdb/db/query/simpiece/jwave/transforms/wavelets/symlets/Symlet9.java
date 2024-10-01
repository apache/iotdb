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
 * Symlet9 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 17.08.2014 14:31:46
 */
public class Symlet9 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym9/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 14:31:46
   */
  public Symlet9() {

    _name = "Symlet 9"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 18; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.0014009155259146807;
    _scalingDeCom[1] = 0.0006197808889855868;
    _scalingDeCom[2] = -0.013271967781817119;
    _scalingDeCom[3] = -0.01152821020767923;
    _scalingDeCom[4] = 0.03022487885827568;
    _scalingDeCom[5] = 0.0005834627461258068;
    _scalingDeCom[6] = -0.05456895843083407;
    _scalingDeCom[7] = 0.238760914607303;
    _scalingDeCom[8] = 0.717897082764412;
    _scalingDeCom[9] = 0.6173384491409358;
    _scalingDeCom[10] = 0.035272488035271894;
    _scalingDeCom[11] = -0.19155083129728512;
    _scalingDeCom[12] = -0.018233770779395985;
    _scalingDeCom[13] = 0.06207778930288603;
    _scalingDeCom[14] = 0.008859267493400484;
    _scalingDeCom[15] = -0.010264064027633142;
    _scalingDeCom[16] = -0.0004731544986800831;
    _scalingDeCom[17] = 0.0010694900329086053;

    _buildOrthonormalSpace();
  } // Symlet9
} // Symlet9
