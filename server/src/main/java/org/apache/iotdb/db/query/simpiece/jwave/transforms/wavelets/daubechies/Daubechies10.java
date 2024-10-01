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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Ingrid Daubechies' orthonormal Daubechies wavelet of 20 coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.02.2014 00:41:08
 */
public class Daubechies10 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db10/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 00:41:08
   */
  public Daubechies10() {

    _name = "Daubechies 10"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 20; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -1.326420300235487e-05;
    _scalingDeCom[1] = 9.358867000108985e-05;
    _scalingDeCom[2] = -0.0001164668549943862;
    _scalingDeCom[3] = -0.0006858566950046825;
    _scalingDeCom[4] = 0.00199240529499085;
    _scalingDeCom[5] = 0.0013953517469940798;
    _scalingDeCom[6] = -0.010733175482979604;
    _scalingDeCom[7] = 0.0036065535669883944;
    _scalingDeCom[8] = 0.03321267405893324;
    _scalingDeCom[9] = -0.02945753682194567;
    _scalingDeCom[10] = -0.07139414716586077;
    _scalingDeCom[11] = 0.09305736460380659;
    _scalingDeCom[12] = 0.12736934033574265;
    _scalingDeCom[13] = -0.19594627437659665;
    _scalingDeCom[14] = -0.24984642432648865;
    _scalingDeCom[15] = 0.2811723436604265;
    _scalingDeCom[16] = 0.6884590394525921;
    _scalingDeCom[17] = 0.5272011889309198;
    _scalingDeCom[18] = 0.18817680007762133;
    _scalingDeCom[19] = 0.026670057900950818;

    //    _scalingDeCom[ 0 ] = -1.326420300235487e-05;
    //    _scalingDeCom[ 1 ] = -9.358867000108985e-05;
    //    _scalingDeCom[ 2 ] = -0.0001164668549943862;
    //    _scalingDeCom[ 3 ] = 0.0006858566950046825;
    //    _scalingDeCom[ 4 ] = 0.00199240529499085;
    //    _scalingDeCom[ 5 ] = -0.0013953517469940798;
    //    _scalingDeCom[ 6 ] = -0.010733175482979604;
    //    _scalingDeCom[ 7 ] = -0.0036065535669883944;
    //    _scalingDeCom[ 8 ] = 0.03321267405893324;
    //    _scalingDeCom[ 9 ] = 0.02945753682194567;
    //    _scalingDeCom[ 10 ] = -0.07139414716586077;
    //    _scalingDeCom[ 11 ] = -0.09305736460380659;
    //    _scalingDeCom[ 12 ] = 0.12736934033574265;
    //    _scalingDeCom[ 13 ] = 0.19594627437659665;
    //    _scalingDeCom[ 14 ] = -0.24984642432648865;
    //    _scalingDeCom[ 15 ] = -0.2811723436604265;
    //    _scalingDeCom[ 16 ] = 0.6884590394525921;
    //    _scalingDeCom[ 17 ] = -0.5272011889309198;
    //    _scalingDeCom[ 18 ] = 0.18817680007762133;
    //    _scalingDeCom[ 19 ] = -0.026670057900950818;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies10
} // Daubechies10
