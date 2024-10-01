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
 * @date 19.08.2014 18:23:15
 */
public class Symlet15 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/sym15/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:23:15
   */
  public Symlet15() {

    _name = "Symlet 15"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 30; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = 9.712419737963348e-06;
    _scalingDeCom[1] = -7.35966679891947e-06;
    _scalingDeCom[2] = -0.00016066186637495343;
    _scalingDeCom[3] = 5.512254785558665e-05;
    _scalingDeCom[4] = 0.0010705672194623959;
    _scalingDeCom[5] = -0.0002673164464718057;
    _scalingDeCom[6] = -0.0035901654473726417;
    _scalingDeCom[7] = 0.003423450736351241;
    _scalingDeCom[8] = 0.01007997708790567;
    _scalingDeCom[9] = -0.01940501143093447;
    _scalingDeCom[10] = -0.03887671687683349;
    _scalingDeCom[11] = 0.021937642719753955;
    _scalingDeCom[12] = 0.04073547969681068;
    _scalingDeCom[13] = -0.04108266663538248;
    _scalingDeCom[14] = 0.11153369514261872;
    _scalingDeCom[15] = 0.5786404152150345;
    _scalingDeCom[16] = 0.7218430296361812;
    _scalingDeCom[17] = 0.2439627054321663;
    _scalingDeCom[18] = -0.1966263587662373;
    _scalingDeCom[19] = -0.1340562984562539;
    _scalingDeCom[20] = 0.06839331006048024;
    _scalingDeCom[21] = 0.06796982904487918;
    _scalingDeCom[22] = -0.008744788886477952;
    _scalingDeCom[23] = -0.01717125278163873;
    _scalingDeCom[24] = 0.0015261382781819983;
    _scalingDeCom[25] = 0.003481028737064895;
    _scalingDeCom[26] = -0.00010815440168545525;
    _scalingDeCom[27] = -0.00040216853760293483;
    _scalingDeCom[28] = 2.171789015077892e-05;
    _scalingDeCom[29] = 2.866070852531808e-05;

    _buildOrthonormalSpace();
  } // Symlet15
} // Symlet15
