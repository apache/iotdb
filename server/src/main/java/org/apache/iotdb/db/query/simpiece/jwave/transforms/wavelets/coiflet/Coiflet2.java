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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.coiflet;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Ingrid Daubechies' orthonormal Coiflet wavelet of 12 coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 15.02.2014 22:33:55
 */
public class Coiflet2 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/coif2/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 22:33:55
   */
  public Coiflet2() {

    _name = "Coiflet 2"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 12; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -0.0007205494453645122;
    _scalingDeCom[1] = -0.0018232088707029932;
    _scalingDeCom[2] = 0.0056114348193944995;
    _scalingDeCom[3] = 0.023680171946334084;
    _scalingDeCom[4] = -0.0594344186464569;
    _scalingDeCom[5] = -0.0764885990783064;
    _scalingDeCom[6] = 0.41700518442169254;
    _scalingDeCom[7] = 0.8127236354455423;
    _scalingDeCom[8] = 0.3861100668211622;
    _scalingDeCom[9] = -0.06737255472196302;
    _scalingDeCom[10] = -0.04146493678175915;
    _scalingDeCom[11] = 0.016387336463522112;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Coiflet2
} // Coiflet2
