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
 * @date 19.08.2014 18:23:12
 */
public class Symlet12 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym12/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:23:12
   */
  public Symlet12() {

    _name = "Symlet 12"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 24; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = 0.00011196719424656033;
    _scalingDeCom[1] = -1.1353928041541452e-05;
    _scalingDeCom[2] = -0.0013497557555715387;
    _scalingDeCom[3] = 0.00018021409008538188;
    _scalingDeCom[4] = 0.007414965517654251;
    _scalingDeCom[5] = -0.0014089092443297553;
    _scalingDeCom[6] = -0.024220722675013445;
    _scalingDeCom[7] = 0.0075537806116804775;
    _scalingDeCom[8] = 0.04917931829966084;
    _scalingDeCom[9] = -0.03584883073695439;
    _scalingDeCom[10] = -0.022162306170337816;
    _scalingDeCom[11] = 0.39888597239022;
    _scalingDeCom[12] = 0.7634790977836572;
    _scalingDeCom[13] = 0.46274103121927235;
    _scalingDeCom[14] = -0.07833262231634322;
    _scalingDeCom[15] = -0.17037069723886492;
    _scalingDeCom[16] = 0.01530174062247884;
    _scalingDeCom[17] = 0.05780417944550566;
    _scalingDeCom[18] = -0.0026043910313322326;
    _scalingDeCom[19] = -0.014589836449234145;
    _scalingDeCom[20] = 0.00030764779631059454;
    _scalingDeCom[21] = 0.002350297614183465;
    _scalingDeCom[22] = -1.8158078862617515e-05;
    _scalingDeCom[23] = -0.0001790665869750869;

    _buildOrthonormalSpace();
  } // Symlet12
} // Symlet12
