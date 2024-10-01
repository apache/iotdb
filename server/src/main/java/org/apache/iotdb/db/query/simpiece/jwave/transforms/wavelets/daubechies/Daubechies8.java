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
 * Ingrid Daubechies' orthonormal wavelet of 16 coefficients and the scales; normed, due to ||*||2 -
 * euclidean norm.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 00:30:05
 */
public class Daubechies8 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db8/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 00:30:05
   */
  public Daubechies8() {

    _name = "Daubechies 8"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 16; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -0.00011747678400228192;
    _scalingDeCom[1] = 0.0006754494059985568;
    _scalingDeCom[2] = -0.0003917403729959771;
    _scalingDeCom[3] = -0.00487035299301066;
    _scalingDeCom[4] = 0.008746094047015655;
    _scalingDeCom[5] = 0.013981027917015516;
    _scalingDeCom[6] = -0.04408825393106472;
    _scalingDeCom[7] = -0.01736930100202211;
    _scalingDeCom[8] = 0.128747426620186;
    _scalingDeCom[9] = 0.00047248457399797254;
    _scalingDeCom[10] = -0.2840155429624281;
    _scalingDeCom[11] = -0.015829105256023893;
    _scalingDeCom[12] = 0.5853546836548691;
    _scalingDeCom[13] = 0.6756307362980128;
    _scalingDeCom[14] = 0.3128715909144659;
    _scalingDeCom[15] = 0.05441584224308161;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies8
} // Daubechies8
