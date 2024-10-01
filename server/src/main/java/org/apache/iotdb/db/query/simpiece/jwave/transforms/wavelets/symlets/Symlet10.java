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
 * @date 17.08.2014 14:35:09
 */
public class Symlet10 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym10/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 14:35:09
   */
  public Symlet10() {

    _name = "Symlet 10"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 20; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.0007701598091144901;
    _scalingDeCom[1] = 9.563267072289475e-05;
    _scalingDeCom[2] = -0.008641299277022422;
    _scalingDeCom[3] = -0.0014653825813050513;
    _scalingDeCom[4] = 0.0459272392310922;
    _scalingDeCom[5] = 0.011609893903711381;
    _scalingDeCom[6] = -0.15949427888491757;
    _scalingDeCom[7] = -0.07088053578324385;
    _scalingDeCom[8] = 0.47169066693843925;
    _scalingDeCom[9] = 0.7695100370211071;
    _scalingDeCom[10] = 0.38382676106708546;
    _scalingDeCom[11] = -0.03553674047381755;
    _scalingDeCom[12] = -0.0319900568824278;
    _scalingDeCom[13] = 0.04999497207737669;
    _scalingDeCom[14] = 0.005764912033581909;
    _scalingDeCom[15] = -0.02035493981231129;
    _scalingDeCom[16] = -0.0008043589320165449;
    _scalingDeCom[17] = 0.004593173585311828;
    _scalingDeCom[18] = 5.7036083618494284e-05;
    _scalingDeCom[19] = -0.0004593294210046588;

    _buildOrthonormalSpace();
  } // Symlet10
} // Symlet10
