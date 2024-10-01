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
 * @date 19.08.2014 18:23:18
 */
public class Symlet18 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym18/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:23:18
   */
  public Symlet18() {

    _name = "Symlet 18"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 36; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = 2.6126125564836423e-06;
    _scalingDeCom[1] = 1.354915761832114e-06;
    _scalingDeCom[2] = -4.5246757874949856e-05;
    _scalingDeCom[3] = -1.4020992577726755e-05;
    _scalingDeCom[4] = 0.00039616840638254753;
    _scalingDeCom[5] = 7.021273459036268e-05;
    _scalingDeCom[6] = -0.002313871814506099;
    _scalingDeCom[7] = -0.00041152110923597756;
    _scalingDeCom[8] = 0.009502164390962365;
    _scalingDeCom[9] = 0.001642986397278216;
    _scalingDeCom[10] = -0.030325091089369604;
    _scalingDeCom[11] = -0.005077085160757053;
    _scalingDeCom[12] = 0.08421992997038655;
    _scalingDeCom[13] = 0.03399566710394736;
    _scalingDeCom[14] = -0.15993814866932407;
    _scalingDeCom[15] = -0.052029158983952786;
    _scalingDeCom[16] = 0.47396905989393956;
    _scalingDeCom[17] = 0.7536291401017928;
    _scalingDeCom[18] = 0.40148386057061813;
    _scalingDeCom[19] = -0.032480573290138676;
    _scalingDeCom[20] = -0.07379920729060717;
    _scalingDeCom[21] = 0.028529597039037808;
    _scalingDeCom[22] = 0.006277944554311694;
    _scalingDeCom[23] = -0.03171268473181454;
    _scalingDeCom[24] = -0.0032607442000749834;
    _scalingDeCom[25] = 0.015012356344250213;
    _scalingDeCom[26] = 0.001087784789595693;
    _scalingDeCom[27] = -0.005239789683026608;
    _scalingDeCom[28] = -0.00018877623940755607;
    _scalingDeCom[29] = 0.0014280863270832796;
    _scalingDeCom[30] = 4.741614518373667e-05;
    _scalingDeCom[31] = -0.0002658301102424104;
    _scalingDeCom[32] = -9.858816030140058e-06;
    _scalingDeCom[33] = 2.955743762093081e-05;
    _scalingDeCom[34] = 7.847298055831765e-07;
    _scalingDeCom[35] = -1.5131530692371587e-06;

    _buildOrthonormalSpace();
  } // Symlet18
} // Symlet18
