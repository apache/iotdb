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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal;

/**
 * BiOrthogonal Wavelet of type 6.8 - Six vanishing moments in wavelet function and eight vanishing
 * moments in scaling function.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 11:42:09
 */
public class BiOrthogonal68 extends BiOrthogonal {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/bior6.8/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 11:42:09
   */
  public BiOrthogonal68() {

    _name = "BiOrthogonal 6/8"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 18; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = 0.;
    _scalingDeCom[1] = 0.0019088317364812906;
    _scalingDeCom[2] = -0.0019142861290887667;
    _scalingDeCom[3] = -0.016990639867602342;
    _scalingDeCom[4] = 0.01193456527972926;
    _scalingDeCom[5] = 0.04973290349094079;
    _scalingDeCom[6] = -0.07726317316720414;
    _scalingDeCom[7] = -0.09405920349573646;
    _scalingDeCom[8] = 0.4207962846098268;
    _scalingDeCom[9] = 0.8259229974584023;
    _scalingDeCom[10] = 0.4207962846098268;
    _scalingDeCom[11] = -0.09405920349573646;
    _scalingDeCom[12] = -0.07726317316720414;
    _scalingDeCom[13] = 0.04973290349094079;
    _scalingDeCom[14] = 0.01193456527972926;
    _scalingDeCom[15] = -0.016990639867602342;
    _scalingDeCom[16] = -0.0019142861290887667;
    _scalingDeCom[17] = 0.0019088317364812906;

    _waveletDeCom = new double[_motherWavelength];
    _waveletDeCom[0] = 0.;
    _waveletDeCom[1] = 0.;
    _waveletDeCom[2] = 0.;
    _waveletDeCom[3] = 0.014426282505624435;
    _waveletDeCom[4] = -0.014467504896790148;
    _waveletDeCom[5] = -0.07872200106262882;
    _waveletDeCom[6] = 0.04036797903033992;
    _waveletDeCom[7] = 0.41784910915027457;
    _waveletDeCom[8] = -0.7589077294536541;
    _waveletDeCom[9] = 0.41784910915027457;
    _waveletDeCom[10] = 0.04036797903033992;
    _waveletDeCom[11] = -0.07872200106262882;
    _waveletDeCom[12] = -0.014467504896790148;
    _waveletDeCom[13] = 0.014426282505624435;
    _waveletDeCom[14] = 0.;
    _waveletDeCom[15] = 0.;
    _waveletDeCom[16] = 0.;
    _waveletDeCom[17] = 0.;

    // build all other coefficients from low & high pass decomposition
    _buildBiOrthonormalSpace();
  } // BiOrthogonal68
} // BiOrthogonal68
