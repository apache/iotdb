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
 * BiOrthogonal Wavelet of type 4.4 - Five vanishing moments in wavelet function and five vanishing
 * moments in scaling function.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 17:40:01
 */
public class BiOrthogonal55 extends BiOrthogonal {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/bior5.5/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 17:40:01
   */
  public BiOrthogonal55() {

    _name = "BiOrthogonal 5/5"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 12; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.;
    _scalingDeCom[1] = 0.;
    _scalingDeCom[2] = 0.03968708834740544;
    _scalingDeCom[3] = 0.007948108637240322;
    _scalingDeCom[4] = -0.05446378846823691;
    _scalingDeCom[5] = 0.34560528195603346;
    _scalingDeCom[6] = 0.7366601814282105;
    _scalingDeCom[7] = 0.34560528195603346;
    _scalingDeCom[8] = -0.05446378846823691;
    _scalingDeCom[9] = 0.007948108637240322;
    _scalingDeCom[10] = 0.03968708834740544;
    _scalingDeCom[11] = 0.;

    _waveletDeCom = new double[_motherWavelength];
    _waveletDeCom[0] = -0.013456709459118716;
    _waveletDeCom[1] = -0.002694966880111507;
    _waveletDeCom[2] = 0.13670658466432914;
    _waveletDeCom[3] = -0.09350469740093886;
    _waveletDeCom[4] = -0.47680326579848425;
    _waveletDeCom[5] = 0.8995061097486484;
    _waveletDeCom[6] = -0.47680326579848425;
    _waveletDeCom[7] = -0.09350469740093886;
    _waveletDeCom[8] = 0.13670658466432914;
    _waveletDeCom[9] = -0.002694966880111507;
    _waveletDeCom[10] = -0.013456709459118716;
    _waveletDeCom[11] = 0.;

    _scalingReCon = new double[_motherWavelength];
    _scalingReCon[0] = 0.013456709459118716;
    _scalingReCon[1] = -0.002694966880111507;
    _scalingReCon[2] = -0.13670658466432914;
    _scalingReCon[3] = -0.09350469740093886;
    _scalingReCon[4] = 0.47680326579848425;
    _scalingReCon[5] = 0.8995061097486484;
    _scalingReCon[6] = 0.47680326579848425;
    _scalingReCon[7] = -0.09350469740093886;
    _scalingReCon[8] = -0.13670658466432914;
    _scalingReCon[9] = -0.002694966880111507;
    _scalingReCon[10] = 0.013456709459118716;
    _scalingReCon[11] = 0.;

    _waveletReCon = new double[_motherWavelength];
    _waveletReCon[0] = 0.;
    _waveletReCon[1] = 0.;
    _waveletReCon[2] = 0.03968708834740544;
    _waveletReCon[3] = -0.007948108637240322;
    _waveletReCon[4] = -0.05446378846823691;
    _waveletReCon[5] = -0.34560528195603346;
    _waveletReCon[6] = 0.7366601814282105;
    _waveletReCon[7] = -0.34560528195603346;
    _waveletReCon[8] = -0.05446378846823691;
    _waveletReCon[9] = -0.007948108637240322;
    _waveletReCon[10] = 0.03968708834740544;
    _waveletReCon[11] = 0.;

    // build all other coefficients from low & high pass decomposition
    // _buildBiOrthonormalSpace( );

  } // BiOrthogonal55
} // BiOrthogonal55
