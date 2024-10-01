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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal;

/**
 * BiOrthogonal Wavelet of type 3.9 - Three vanishing moments in wavelet function and nine vanishing
 * moments in scaling function.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 17:25:01
 */
public class BiOrthogonal39 extends BiOrthogonal {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/bior3.9/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 17:25:01
   */
  public BiOrthogonal39() {

    _name = "BiOrthogonal 3/9"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 20; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -0.000679744372783699;
    _scalingDeCom[1] = 0.002039233118351097;
    _scalingDeCom[2] = 0.005060319219611981;
    _scalingDeCom[3] = -0.020618912641105536;
    _scalingDeCom[4] = -0.014112787930175846;
    _scalingDeCom[5] = 0.09913478249423216;
    _scalingDeCom[6] = 0.012300136269419315;
    _scalingDeCom[7] = -0.32019196836077857;
    _scalingDeCom[8] = 0.0020500227115698858;
    _scalingDeCom[9] = 0.9421257006782068;
    _scalingDeCom[10] = 0.9421257006782068;
    _scalingDeCom[11] = 0.0020500227115698858;
    _scalingDeCom[12] = -0.32019196836077857;
    _scalingDeCom[13] = 0.012300136269419315;
    _scalingDeCom[14] = 0.09913478249423216;
    _scalingDeCom[15] = -0.014112787930175846;
    _scalingDeCom[16] = -0.020618912641105536;
    _scalingDeCom[17] = 0.005060319219611981;
    _scalingDeCom[18] = 0.002039233118351097;
    _scalingDeCom[19] = -0.000679744372783699;

    _waveletDeCom = new double[_motherWavelength];
    _waveletDeCom[0] = 0.;
    _waveletDeCom[1] = 0.;
    _waveletDeCom[2] = 0.;
    _waveletDeCom[3] = 0.;
    _waveletDeCom[4] = 0.;
    _waveletDeCom[5] = 0.;
    _waveletDeCom[6] = 0.;
    _waveletDeCom[7] = 0.;
    _waveletDeCom[8] = -0.1767766952966369;
    _waveletDeCom[9] = 0.5303300858899107;
    _waveletDeCom[10] = -0.5303300858899107;
    _waveletDeCom[11] = 0.1767766952966369;
    _waveletDeCom[12] = 0.;
    _waveletDeCom[13] = 0.;
    _waveletDeCom[14] = 0.;
    _waveletDeCom[15] = 0.;
    _waveletDeCom[16] = 0.;
    _waveletDeCom[17] = 0.;
    _waveletDeCom[18] = 0.;
    _waveletDeCom[19] = 0.;

    // build all other coefficients from low & high pass decomposition
    _buildBiOrthonormalSpace();
  } // BiOrthogonal39
} // BiOrthogonal39
