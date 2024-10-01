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
 * Ingrid Daubechies' orthonormal Daubechies wavelet of 34 coefficients.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 19.08.2014 18:10:58
 */
public class Daubechies17 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db17/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 19.08.2014 18:10:58
   */
  public Daubechies17() {

    _name = "Daubechies 17"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 34; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];

    _scalingDeCom[0] = 7.26749296856637e-09;
    _scalingDeCom[1] = -8.423948446008154e-08;
    _scalingDeCom[2] = 2.9577009333187617e-07;
    _scalingDeCom[3] = 3.0165496099963414e-07;
    _scalingDeCom[4] = -4.505942477225963e-06;
    _scalingDeCom[5] = 6.990600985081294e-06;
    _scalingDeCom[6] = 2.318681379876164e-05;
    _scalingDeCom[7] = -8.204803202458212e-05;
    _scalingDeCom[8] = -2.5610109566546042e-05;
    _scalingDeCom[9] = 0.0004394654277689454;
    _scalingDeCom[10] = -0.00032813251941022427;
    _scalingDeCom[11] = -0.001436845304805;
    _scalingDeCom[12] = 0.0023012052421511474;
    _scalingDeCom[13] = 0.002967996691518064;
    _scalingDeCom[14] = -0.008602921520347815;
    _scalingDeCom[15] = -0.0030429899813869555;
    _scalingDeCom[16] = 0.022733676583919053;
    _scalingDeCom[17] = -0.0032709555358783646;
    _scalingDeCom[18] = -0.04692243838937891;
    _scalingDeCom[19] = 0.022312336178011833;
    _scalingDeCom[20] = 0.08110598665408082;
    _scalingDeCom[21] = -0.05709141963185808;
    _scalingDeCom[22] = -0.12681569177849797;
    _scalingDeCom[23] = 0.10113548917744287;
    _scalingDeCom[24] = 0.19731058956508457;
    _scalingDeCom[25] = -0.12659975221599248;
    _scalingDeCom[26] = -0.32832074836418546;
    _scalingDeCom[27] = 0.027314970403312946;
    _scalingDeCom[28] = 0.5183157640572823;
    _scalingDeCom[29] = 0.6109966156850273;
    _scalingDeCom[30] = 0.3703507241528858;
    _scalingDeCom[31] = 0.13121490330791097;
    _scalingDeCom[32] = 0.025985393703623173;
    _scalingDeCom[33] = 0.00224180700103879;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies17
} // Daubechies17
