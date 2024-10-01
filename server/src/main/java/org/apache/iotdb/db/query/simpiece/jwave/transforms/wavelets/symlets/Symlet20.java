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
 * Symlet20 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 13:47:56
 */
public class Symlet20 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym20/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 13:47:56
   */
  public Symlet20() {

    _name = "Symlet 20"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 40; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 3.695537474835221e-07;
    _scalingDeCom[1] = -1.9015675890554106e-07;
    _scalingDeCom[2] = -7.919361411976999e-06;
    _scalingDeCom[3] = 3.025666062736966e-06;
    _scalingDeCom[4] = 7.992967835772481e-05;
    _scalingDeCom[5] = -1.928412300645204e-05;
    _scalingDeCom[6] = -0.0004947310915672655;
    _scalingDeCom[7] = 7.215991188074035e-05;
    _scalingDeCom[8] = 0.002088994708190198;
    _scalingDeCom[9] = -0.0003052628317957281;
    _scalingDeCom[10] = -0.006606585799088861;
    _scalingDeCom[11] = 0.0014230873594621453;
    _scalingDeCom[12] = 0.01700404902339034;
    _scalingDeCom[13] = -0.003313857383623359;
    _scalingDeCom[14] = -0.031629437144957966;
    _scalingDeCom[15] = 0.008123228356009682;
    _scalingDeCom[16] = 0.025579349509413946;
    _scalingDeCom[17] = -0.07899434492839816;
    _scalingDeCom[18] = -0.02981936888033373;
    _scalingDeCom[19] = 0.4058314443484506;
    _scalingDeCom[20] = 0.75116272842273;
    _scalingDeCom[21] = 0.47199147510148703;
    _scalingDeCom[22] = -0.0510883429210674;
    _scalingDeCom[23] = -0.16057829841525254;
    _scalingDeCom[24] = 0.03625095165393308;
    _scalingDeCom[25] = 0.08891966802819956;
    _scalingDeCom[26] = -0.0068437019650692274;
    _scalingDeCom[27] = -0.035373336756604236;
    _scalingDeCom[28] = 0.0019385970672402002;
    _scalingDeCom[29] = 0.012157040948785737;
    _scalingDeCom[30] = -0.0006111263857992088;
    _scalingDeCom[31] = -0.0034716478028440734;
    _scalingDeCom[32] = 0.0001254409172306726;
    _scalingDeCom[33] = 0.0007476108597820572;
    _scalingDeCom[34] = -2.6615550335516086e-05;
    _scalingDeCom[35] = -0.00011739133516291466;
    _scalingDeCom[36] = 4.525422209151636e-06;
    _scalingDeCom[37] = 1.22872527779612e-05;
    _scalingDeCom[38] = -3.2567026420174407e-07;
    _scalingDeCom[39] = -6.329129044776395e-07;

    _buildOrthonormalSpace();
  } // Symlet20(
} // Symlet20(
