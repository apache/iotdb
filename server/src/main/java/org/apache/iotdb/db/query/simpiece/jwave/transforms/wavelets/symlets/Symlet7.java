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
 * Symlet7 filter: near symmetric, orthogonal (orthonormal), biorthogonal.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 17.08.2014 14:24:20
 */
public class Symlet7 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym7/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 14:24:20
   */
  public Symlet7() {

    _name = "Symlet 7"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 14; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.002681814568257878;
    _scalingDeCom[1] = -0.0010473848886829163;
    _scalingDeCom[2] = -0.01263630340325193;
    _scalingDeCom[3] = 0.03051551316596357;
    _scalingDeCom[4] = 0.0678926935013727;
    _scalingDeCom[5] = -0.049552834937127255;
    _scalingDeCom[6] = 0.017441255086855827;
    _scalingDeCom[7] = 0.5361019170917628;
    _scalingDeCom[8] = 0.767764317003164;
    _scalingDeCom[9] = 0.2886296317515146;
    _scalingDeCom[10] = -0.14004724044296152;
    _scalingDeCom[11] = -0.10780823770381774;
    _scalingDeCom[12] = 0.004010244871533663;
    _scalingDeCom[13] = 0.010268176708511255;

    _buildOrthonormalSpace();
  } // Symlet7
} // Symlet7
