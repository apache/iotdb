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
 * Symlet10 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 19.08.2014 18:23:19
 */
public class Symlet19 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym19/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:23:19
   */
  public Symlet19() {

    _name = "Symlet 19"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 38; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = 5.487732768215838e-07;
    _scalingDeCom[1] = -6.463651303345963e-07;
    _scalingDeCom[2] = -1.1880518269823984e-05;
    _scalingDeCom[3] = 8.873312173729286e-06;
    _scalingDeCom[4] = 0.0001155392333357879;
    _scalingDeCom[5] = -4.612039600210587e-05;
    _scalingDeCom[6] = -0.000635764515004334;
    _scalingDeCom[7] = 0.00015915804768084938;
    _scalingDeCom[8] = 0.0021214250281823303;
    _scalingDeCom[9] = -0.0011607032572062486;
    _scalingDeCom[10] = -0.005122205002583014;
    _scalingDeCom[11] = 0.007968438320613306;
    _scalingDeCom[12] = 0.01579743929567463;
    _scalingDeCom[13] = -0.02265199337824595;
    _scalingDeCom[14] = -0.046635983534938946;
    _scalingDeCom[15] = 0.0070155738571741596;
    _scalingDeCom[16] = 0.008954591173043624;
    _scalingDeCom[17] = -0.06752505804029409;
    _scalingDeCom[18] = 0.10902582508127781;
    _scalingDeCom[19] = 0.578144945338605;
    _scalingDeCom[20] = 0.7195555257163943;
    _scalingDeCom[21] = 0.2582661692372836;
    _scalingDeCom[22] = -0.17659686625203097;
    _scalingDeCom[23] = -0.11624173010739675;
    _scalingDeCom[24] = 0.09363084341589714;
    _scalingDeCom[25] = 0.08407267627924504;
    _scalingDeCom[26] = -0.016908234861345205;
    _scalingDeCom[27] = -0.02770989693131125;
    _scalingDeCom[28] = 0.004319351874894969;
    _scalingDeCom[29] = 0.008262236955528255;
    _scalingDeCom[30] = -0.0006179223277983108;
    _scalingDeCom[31] = -0.0017049602611649971;
    _scalingDeCom[32] = 0.00012930767650701415;
    _scalingDeCom[33] = 0.0002762187768573407;
    _scalingDeCom[34] = -1.6821387029373716e-05;
    _scalingDeCom[35] = -2.8151138661550245e-05;
    _scalingDeCom[36] = 2.0623170632395688e-06;
    _scalingDeCom[37] = 1.7509367995348687e-06;

    _buildOrthonormalSpace();
  } // Symlet19
} // Symlet19
