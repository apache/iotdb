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
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.haar;

import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Alfred Haar's orthogonal wavelet transform.
 *
 * @date 03.06.2010 09:47:24
 * @author Christian (graetz23@gmail.com)
 */
public class Haar1Orthogonal extends Wavelet {

  /** energy correction factor of the (overwritten) reverse transform */
  private static double _energyCorrectionFactor = .5;

  /**
   * Constructor setting up the orthogonal Haar scaling coefficients and matching wavelet
   * coefficients. However, the reverse method has to be obverloaded, due to having an change in the
   * energy that has to be corrected while perfoming the reconstruction.!
   *
   * @date 03.06.2010 09:47:24
   * @author Christian (graetz23@gmail.com)
   */
  public Haar1Orthogonal() {

    // Remark on mathematics (perpendicular, orthogonal, and orthonormal):
    //
    // "Orthogonal" is used for vectors which are perpendicular but of any length.
    // "Orthonormal" is used for vectors which are perpendicular and of a unit length of one.
    //
    //
    // "Orthogonal" system -- ASCII art does not display the angles in 90 deg (or 45 deg):
    //
    //    ^ y
    //    |
    //  1 +      .  scaling function
    //    |    /    {1,1}  \
    //    |  /             | length = 1.4142135623730951
    //    |/               /        = sqrt( (1)^2 + (1)^2 )
    //  --o------+-> x
    //  0 |\     1         \
    //    |  \             | length = 1.4142135623730951
    //    |    \           /        = sqrt( (1)^2 + (-1)^2 )
    // -1 +      .  wavelet function
    //    |         {1,-1}
    //
    // You can see that by each step of the algorithm the input coefficients "energy"
    // (energy := ||.||_2 euclidean norm) rises, while ever input value is multiplied
    // by 1.414213 (sqrt(2)). However, one has to correct this change of "energy" in
    // the reverse transform method by multiplying the factor 1/2.
    //
    // The factor is used by the private member variable _energyCorrectionFactor and
    // that changes the (overwritten) reverse transform; see methods of this class.
    //
    // For the discussed euclidean norm; see http://en.wikipedia.org/wiki/Euclidean_norm
    //
    // The main disadvantage using an "orthogonal" wavelets is that the generated wavelet
    // sub spaces of different levels can not be combined anymore easily, due to their
    // different "energy" or norm (||.||_2). If an "orthonormal" wavelet is taken, the
    // ||.||_2 norm does not change the energy at any level or any transform level. This
    // allows for combining wavelet sub spaces of different levels easily!
    //
    // Other common used orthogonal Haar coefficients:
    //
    // _scalingDeCom[ 0 ] = .5; // s0
    // _scalingDeCom[ 1 ] = .5; //  s1
    //
    // _waveletDeCom[ 0 ] = .5; // w0 = s1
    // _waveletDeCom[ 1 ] = -.5; // w1 = -s0
    //
    // _scalingReCon[ 0 ] = .5; // s0
    // _scalingReCon[ 1 ] = .5; //  s1
    //
    // _waveletReCon[ 0 ] = .5; // w0 = s1
    // _waveletReCon[ 1 ] = -.5; // w1 = -s0
    //
    // The ||.||_2 norm will shrink the energy compared to the input signal's norm,
    // due to length sqrt( .5 * .5 + .5 * .5 ) = sqrt( .5 ) = .7071. Therefore,
    // exchange the factor in the reverse method as implemented 1/2 = .5  to 2.!!!
    //
    // Another alternative is to used mixed coefficients like:
    //
    // _scalingDeCom[ 0 ] = 1.; // s_d0
    // _scalingDeCom[ 1 ] = 1.; //  s_d1
    //
    // _waveletDeCom[ 0 ] = 1.; // w_d0 = s_d1
    // _waveletDeCom[ 1 ] = -1.; // w_d1 = -s_d0
    //
    // _scalingReCon[ 0 ] = .5; // s_r0
    // _scalingReCon[ 1 ] = .5; //  s_r1
    //
    // _waveletReCon[ 0 ] = .5; // w_r0 = s_r1
    // _waveletReCon[ 1 ] = -.5; // w_r1 = -s_r0
    //
    // or
    //
    // _scalingDeCom[ 0 ] = .5; // s_d0
    // _scalingDeCom[ 1 ] = .5; //  s_d1
    //
    // _waveletDeCom[ 0 ] = .5; // w_d0 = s_d1
    // _waveletDeCom[ 1 ] = -.5; // w_d1 = -s_d0
    //
    // _scalingReCon[ 0 ] = 2.; // s_r0
    // _scalingReCon[ 1 ] = 2.; //  s_r1
    //
    // _waveletReCon[ 0 ] = 2.; // w_r0 = s_r1
    // _waveletReCon[ 1 ] = -2.; // w_r1 = -s_r0
    //
    // Have fun ~8>

    _name = "Haar orthogonal"; // name of the wavelet

    _transformWavelength = 2; // minimal wavelength of input signal

    _motherWavelength = 2; // wavelength of mother wavelet

    // Orthogonal wavelet coefficients; NOT orthonormal, due to missing sqrt(2.)
    _scalingDeCom = new double[_motherWavelength]; // can be done in static way also; faster?
    _scalingDeCom[0] = 1.; // w0
    _scalingDeCom[1] = 1.; //  w1

    // Rule for constructing an orthogonal vector in R^2 -- scales
    _waveletDeCom = new double[_motherWavelength]; // can be done in static way also; faster?
    _waveletDeCom[0] = _scalingDeCom[1]; // w1
    _waveletDeCom[1] = -_scalingDeCom[0]; // -w0

    // Copy to reconstruction filters due to orthogonality (orthonormality)!
    _scalingReCon = new double[_motherWavelength];
    _waveletReCon = new double[_motherWavelength];
    for (int i = 0; i < _motherWavelength; i++) {

      _scalingReCon[i] = _scalingDeCom[i];
      _waveletReCon[i] = _waveletDeCom[i];
    } // i
  } // Haar1

  /**
   * The reverse wavelet transform using the Alfred Haar's wavelet. The arrHilb array keeping
   * coefficients of Hilbert domain should be of length 2 to the power of p -- length = 2^p where p
   * is a positive integer. But in case of an only orthogonal Haar wavelet the reverse transform has
   * to have a factor of 0.5 to reduce the up sampled "energy" in Hilbert space.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 21:17:22
   */
  @Override
  public double[] reverse(double[] arrHilb, int arrHilbLength) {

    double[] arrTime = new double[arrHilbLength];

    for (int i = 0; i < arrTime.length; i++) arrTime[i] = 0.;

    int h = arrTime.length >> 1; // .. -> 8 -> 4 -> 2 .. shrinks in each step by half wavelength

    for (int i = 0; i < h; i++) {

      for (int j = 0; j < _motherWavelength; j++) {

        int k = (i * 2) + j; // int k = ( i << 1 ) + j;

        while (k >= arrTime.length)
          k -= arrTime.length; // circulate over arrays if scaling and wavelet are larger

        // adding up energy from scaling coefficients, the low pass (approximation) filter, and
        // wavelet coefficients, the high pass filter (details). However, the raised energy has
        // to be reduced by half for each step because of vectorial length of each base vector
        // of the orthogonal system is of sqrt( 2. ).
        arrTime[k] +=
            _energyCorrectionFactor
                * ((arrHilb[i] * _scalingReCon[j]) + (arrHilb[i + h] * _waveletReCon[j]));
      } // Reconstruction from patterns of: { scaling coefficients | wavelet coefficients }
    } // h = 2^(p-1) | p = { 1, 2, .., N } .. shrink in each step by half wavelength

    return arrTime;
  } // reverse
} // Haar1Orthogonal
