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
 * Ingrid Daubechies' orthonormal Daubechies wavelet of 26 coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 19.08.2014 18:10:54
 */
public class Daubechies13 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db13/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:10:54
   */
  public Daubechies13() {

    _name = "Daubechies 13"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 26; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = 5.2200350984548e-07;
    _scalingDeCom[1] = -4.700416479360808e-06;
    _scalingDeCom[2] = 1.0441930571407941e-05;
    _scalingDeCom[3] = 3.067853757932436e-05;
    _scalingDeCom[4] = -0.0001651289885565057;
    _scalingDeCom[5] = 4.9251525126285676e-05;
    _scalingDeCom[6] = 0.000932326130867249;
    _scalingDeCom[7] = -0.0013156739118922766;
    _scalingDeCom[8] = -0.002761911234656831;
    _scalingDeCom[9] = 0.007255589401617119;
    _scalingDeCom[10] = 0.003923941448795577;
    _scalingDeCom[11] = -0.02383142071032781;
    _scalingDeCom[12] = 0.002379972254052227;
    _scalingDeCom[13] = 0.056139477100276156;
    _scalingDeCom[14] = -0.026488406475345658;
    _scalingDeCom[15] = -0.10580761818792761;
    _scalingDeCom[16] = 0.07294893365678874;
    _scalingDeCom[17] = 0.17947607942935084;
    _scalingDeCom[18] = -0.12457673075080665;
    _scalingDeCom[19] = -0.31497290771138414;
    _scalingDeCom[20] = 0.086985726179645;
    _scalingDeCom[21] = 0.5888895704312119;
    _scalingDeCom[22] = 0.6110558511587811;
    _scalingDeCom[23] = 0.3119963221604349;
    _scalingDeCom[24] = 0.08286124387290195;
    _scalingDeCom[25] = 0.009202133538962279;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies13
} // Daubechies13
