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
 * @date 16.02.2014 12:06:47
 */
public class Daubechies20 extends Wavelet {

  /**
   * Already orthonormal coefficients taken from Filip Wasilewski's webpage
   * http://wavelets.pybytes.com/wavelet/db20/ Thanks!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.02.2014 12:06:47
   */
  public Daubechies20() {

    _name = "Daubechies 20"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 40; // wavelength of mother wavelet

    _scalingDeCom = new double[_motherWavelength];
    _scalingDeCom[0] = -2.998836489615753e-10;
    _scalingDeCom[1] = 4.05612705554717e-09;
    _scalingDeCom[2] = -1.814843248297622e-08;
    _scalingDeCom[3] = 2.0143220235374613e-10;
    _scalingDeCom[4] = 2.633924226266962e-07;
    _scalingDeCom[5] = -6.847079596993149e-07;
    _scalingDeCom[6] = -1.0119940100181473e-06;
    _scalingDeCom[7] = 7.241248287663791e-06;
    _scalingDeCom[8] = -4.376143862182197e-06;
    _scalingDeCom[9] = -3.710586183390615e-05;
    _scalingDeCom[10] = 6.774280828373048e-05;
    _scalingDeCom[11] = 0.00010153288973669777;
    _scalingDeCom[12] = -0.0003851047486990061;
    _scalingDeCom[13] = -5.349759844340453e-05;
    _scalingDeCom[14] = 0.0013925596193045254;
    _scalingDeCom[15] = -0.0008315621728772474;
    _scalingDeCom[16] = -0.003581494259744107;
    _scalingDeCom[17] = 0.00442054238676635;
    _scalingDeCom[18] = 0.0067216273018096935;
    _scalingDeCom[19] = -0.013810526137727442;
    _scalingDeCom[20] = -0.008789324924555765;
    _scalingDeCom[21] = 0.03229429953011916;
    _scalingDeCom[22] = 0.0058746818113949465;
    _scalingDeCom[23] = -0.061722899624668884;
    _scalingDeCom[24] = 0.005632246857685454;
    _scalingDeCom[25] = 0.10229171917513397;
    _scalingDeCom[26] = -0.024716827337521424;
    _scalingDeCom[27] = -0.1554587507060453;
    _scalingDeCom[28] = 0.039850246458519104;
    _scalingDeCom[29] = 0.22829105082013823;
    _scalingDeCom[30] = -0.016727088308801888;
    _scalingDeCom[31] = -0.3267868004335376;
    _scalingDeCom[32] = -0.13921208801128787;
    _scalingDeCom[33] = 0.36150229873889705;
    _scalingDeCom[34] = 0.6104932389378558;
    _scalingDeCom[35] = 0.4726961853103315;
    _scalingDeCom[36] = 0.21994211355113222;
    _scalingDeCom[37] = 0.06342378045900529;
    _scalingDeCom[38] = 0.010549394624937735;
    _scalingDeCom[39] = 0.0007799536136659112;

    _buildOrthonormalSpace(); // build all other coefficients from low pass decomposition
  } // Daubechies20
} // Daubechies20
