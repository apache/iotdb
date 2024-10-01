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
 * Ingrid Daubechies' orthonormal wavelet of 14 coefficients and the scales; normed, due to ||*||2 -
 * euclidean norm.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 00:26:36
 */
public class Daubechies7 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db7/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 00:26:36
   */
  public Daubechies7() {

    _name = "Daubechies 7"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 14; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.0003537138000010399;
    _scalingDeCom[1] = -0.0018016407039998328;
    _scalingDeCom[2] = 0.00042957797300470274;
    _scalingDeCom[3] = 0.012550998556013784;
    _scalingDeCom[4] = -0.01657454163101562;
    _scalingDeCom[5] = -0.03802993693503463;
    _scalingDeCom[6] = 0.0806126091510659;
    _scalingDeCom[7] = 0.07130921926705004;
    _scalingDeCom[8] = -0.22403618499416572;
    _scalingDeCom[9] = -0.14390600392910627;
    _scalingDeCom[10] = 0.4697822874053586;
    _scalingDeCom[11] = 0.7291320908465551;
    _scalingDeCom[12] = 0.39653931948230575;
    _scalingDeCom[13] = 0.07785205408506236;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies7
} // Daubechies7
