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
 * BiOrthogonal Wavelet of type 4.4 - Four vanishing moments in wavelet function and four vanishing
 * moments in scaling function - 44 muhahaha! ~8>
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 17:36:17
 */
public class BiOrthogonal44 extends BiOrthogonal {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/bior4.4/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 17:36:17
   */
  public BiOrthogonal44() {

    _name = "BiOrthogonal 4/4"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 10; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.;
    _scalingDeCom[1] = 0.03782845550726404;
    _scalingDeCom[2] = -0.023849465019556843;
    _scalingDeCom[3] = -0.11062440441843718;
    _scalingDeCom[4] = 0.37740285561283066;
    _scalingDeCom[5] = 0.8526986790088938;
    _scalingDeCom[6] = 0.37740285561283066;
    _scalingDeCom[7] = -0.11062440441843718;
    _scalingDeCom[8] = -0.023849465019556843;
    _scalingDeCom[9] = 0.03782845550726404;

    _waveletDeCom = new double[_motherWavelength];
    _waveletDeCom[0] = 0.;
    _waveletDeCom[1] = -0.06453888262869706;
    _waveletDeCom[2] = 0.04068941760916406;
    _waveletDeCom[3] = 0.41809227322161724;
    _waveletDeCom[4] = -0.7884856164055829;
    _waveletDeCom[5] = 0.41809227322161724;
    _waveletDeCom[6] = 0.04068941760916406;
    _waveletDeCom[7] = -0.06453888262869706;
    _waveletDeCom[8] = 0.;
    _waveletDeCom[9] = 0.;

    // build all other coefficients from low & high pass decomposition
    _buildBiOrthonormalSpace();
  } // BiOrthogonal44
} // BiOrthogonal44
