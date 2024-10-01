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
 * Symlet2 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 13:40:30
 */
public class Symlet2 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym2/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 13:40:30
   */
  public Symlet2() {

    _name = "Symlet 2"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 4; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -0.12940952255092145;
    _scalingDeCom[1] = 0.22414386804185735;
    _scalingDeCom[2] = 0.836516303737469;
    _scalingDeCom[3] = 0.48296291314469025;

    //    _waveletDeCom = new double[ _motherWavelength ];
    //    _waveletDeCom[ 0 ] = -0.48296291314469025;
    //    _waveletDeCom[ 1 ] = 0.836516303737469;
    //    _waveletDeCom[ 2 ] = -0.22414386804185735;
    //    _waveletDeCom[ 3 ] = -0.12940952255092145;
    //
    //    _scalingReCon = new double[ _motherWavelength ];
    //    _scalingReCon[ 0 ] = 0.48296291314469025;
    //    _scalingReCon[ 1 ] = 0.836516303737469;
    //    _scalingReCon[ 2 ] = 0.22414386804185735;
    //    _scalingReCon[ 3 ] = -0.12940952255092145;
    //
    //    _waveletReCon = new double[ _motherWavelength ];
    //    _waveletReCon[ 0 ] = -0.12940952255092145;
    //    _waveletReCon[ 1 ] = -0.22414386804185735;
    //    _waveletReCon[ 2 ] = 0.836516303737469;
    //    _waveletReCon[ 3 ] = -0.48296291314469025;

    _buildOrthonormalSpace();
  } // Symlet2
} // Symlet2
