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
 * Ingrid Daubechies' orthonormal Daubechies wavelet of 30 coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 19.08.2014 18:10:56
 */
public class Daubechies15 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db15/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:10:56
   */
  public Daubechies15() {

    _name = "Daubechies 15"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 30; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = 6.133359913303714e-08;
    _scalingDeCom[1] = -6.316882325879451e-07;
    _scalingDeCom[2] = 1.8112704079399406e-06;
    _scalingDeCom[3] = 3.3629871817363823e-06;
    _scalingDeCom[4] = -2.8133296266037558e-05;
    _scalingDeCom[5] = 2.579269915531323e-05;
    _scalingDeCom[6] = 0.00015589648992055726;
    _scalingDeCom[7] = -0.00035956524436229364;
    _scalingDeCom[8] = -0.0003734823541372647;
    _scalingDeCom[9] = 0.0019433239803823459;
    _scalingDeCom[10] = -0.00024175649075894543;
    _scalingDeCom[11] = -0.0064877345603061454;
    _scalingDeCom[12] = 0.005101000360422873;
    _scalingDeCom[13] = 0.015083918027862582;
    _scalingDeCom[14] = -0.020810050169636805;
    _scalingDeCom[15] = -0.02576700732836694;
    _scalingDeCom[16] = 0.054780550584559995;
    _scalingDeCom[17] = 0.033877143923563204;
    _scalingDeCom[18] = -0.11112093603713753;
    _scalingDeCom[19] = -0.0396661765557336;
    _scalingDeCom[20] = 0.19014671400708816;
    _scalingDeCom[21] = 0.06528295284876569;
    _scalingDeCom[22] = -0.28888259656686216;
    _scalingDeCom[23] = -0.19320413960907623;
    _scalingDeCom[24] = 0.33900253545462167;
    _scalingDeCom[25] = 0.6458131403572103;
    _scalingDeCom[26] = 0.4926317717079753;
    _scalingDeCom[27] = 0.20602386398692688;
    _scalingDeCom[28] = 0.04674339489275062;
    _scalingDeCom[29] = 0.004538537361577376;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies15
} // Daubechies15
