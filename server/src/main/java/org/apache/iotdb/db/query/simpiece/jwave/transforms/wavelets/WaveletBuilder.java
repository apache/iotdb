/**
 * Create Wavelet objects ...
 *
 * @author Christian (graetz23@gmail.com)
 * @date 14.03.2015 13:50:30
 *     <p>WaveletBuilder.java
 */
package org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets;

import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal11;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal13;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal15;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal22;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal24;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal26;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal28;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal31;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal33;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal35;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal37;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal39;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal44;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal55;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.biorthogonal.BiOrthogonal68;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.coiflet.Coiflet1;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.coiflet.Coiflet2;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.coiflet.Coiflet3;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.coiflet.Coiflet4;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.coiflet.Coiflet5;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies10;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies11;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies12;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies13;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies14;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies15;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies16;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies17;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies18;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies19;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies2;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies20;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies3;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies4;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies5;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies6;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies7;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies8;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.daubechies.Daubechies9;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.haar.Haar1;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.haar.Haar1Orthogonal;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.legendre.Legendre1;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.legendre.Legendre2;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.legendre.Legendre3;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.other.DiscreteMayer;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet10;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet11;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet12;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet13;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet14;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet15;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet16;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet17;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet18;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet19;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet2;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet20;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet3;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet4;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet5;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet6;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet7;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet8;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.symlets.Symlet9;

import java.util.ArrayList;

/**
 * Class for creating and identifying Wavelet object.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 14.03.2015 13:50:30
 */
public class WaveletBuilder {

  /**
   * Create a Wavelet object by string. Look into each Wavelet for matching string identifier. By
   * the way the method requires Java 7, due to the switch statement with at String. *rofl*
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 14:19:09
   * @param waveletName identifier as stored in Wavelet object
   * @return a matching object of type Wavelet
   */
  public static Wavelet create(String waveletName) {

    Wavelet wavelet = null;

    try {

      switch (waveletName) {
        case "Haar":
          wavelet = new Haar1();
          break;

        case "Haar orthogonal":
          wavelet = new Haar1Orthogonal();
          break;

        case "Daubechies 2":
          wavelet = new Daubechies2();
          break;

        case "Daubechies 3":
          wavelet = new Daubechies3();
          break;

        case "Daubechies 4":
          wavelet = new Daubechies4();
          break;

        case "Daubechies 5":
          wavelet = new Daubechies5();
          break;

        case "Daubechies 6":
          wavelet = new Daubechies6();
          break;

        case "Daubechies 7":
          wavelet = new Daubechies7();
          break;

        case "Daubechies 8":
          wavelet = new Daubechies8();
          break;

        case "Daubechies 9":
          wavelet = new Daubechies9();
          break;

        case "Daubechies 10":
          wavelet = new Daubechies10();
          break;

        case "Daubechies 11":
          wavelet = new Daubechies11();
          break;

        case "Daubechies 12":
          wavelet = new Daubechies12();
          break;

        case "Daubechies 13":
          wavelet = new Daubechies13();
          break;

        case "Daubechies 14":
          wavelet = new Daubechies14();
          break;

        case "Daubechies 15":
          wavelet = new Daubechies15();
          break;

        case "Daubechies 16":
          wavelet = new Daubechies16();
          break;

        case "Daubechies 17":
          wavelet = new Daubechies17();
          break;

        case "Daubechies 18":
          wavelet = new Daubechies18();
          break;

        case "Daubechies 19":
          wavelet = new Daubechies19();
          break;

        case "Daubechies 20":
          wavelet = new Daubechies20();
          break;

        case "Coiflet 1":
          wavelet = new Coiflet1();
          break;

        case "Coiflet 2":
          wavelet = new Coiflet2();
          break;

        case "Coiflet 3":
          wavelet = new Coiflet3();
          break;

        case "Coiflet 4":
          wavelet = new Coiflet4();
          break;

        case "Coiflet 5":
          wavelet = new Coiflet5();
          break;

        case "Legendre 1":
          wavelet = new Legendre1();
          break;

        case "Legendre 2":
          wavelet = new Legendre2();
          break;

        case "Legendre 3":
          wavelet = new Legendre3();
          break;

        case "Symlet 2":
          wavelet = new Symlet2();
          break;

        case "Symlet 3":
          wavelet = new Symlet3();
          break;

        case "Symlet 4":
          wavelet = new Symlet4();
          break;

        case "Symlet 5":
          wavelet = new Symlet5();
          break;

        case "Symlet 6":
          wavelet = new Symlet6();
          break;

        case "Symlet 7":
          wavelet = new Symlet7();
          break;

        case "Symlet 8":
          wavelet = new Symlet8();
          break;

        case "Symlet 9":
          wavelet = new Symlet9();
          break;

        case "Symlet 10":
          wavelet = new Symlet10();
          break;

        case "Symlet 11":
          wavelet = new Symlet11();
          break;

        case "Symlet 12":
          wavelet = new Symlet12();
          break;

        case "Symlet 13":
          wavelet = new Symlet13();
          break;

        case "Symlet 14":
          wavelet = new Symlet14();
          break;

        case "Symlet 15":
          wavelet = new Symlet15();
          break;

        case "Symlet 16":
          wavelet = new Symlet16();
          break;

        case "Symlet 17":
          wavelet = new Symlet17();
          break;

        case "Symlet 18":
          wavelet = new Symlet18();
          break;

        case "Symlet 19":
          wavelet = new Symlet19();
          break;

        case "Symlet 20":
          wavelet = new Symlet20();
          break;

        case "BiOrthogonal 1/1":
          wavelet = new BiOrthogonal11();
          break;

        case "BiOrthogonal 1/3":
          wavelet = new BiOrthogonal13();
          break;

        case "BiOrthogonal 1/5":
          wavelet = new BiOrthogonal15();
          break;

        case "BiOrthogonal 2/2":
          wavelet = new BiOrthogonal22();
          break;

        case "BiOrthogonal 2/4":
          wavelet = new BiOrthogonal24();
          break;

        case "BiOrthogonal 2/6":
          wavelet = new BiOrthogonal26();
          break;

        case "BiOrthogonal 2/8":
          wavelet = new BiOrthogonal28();
          break;

        case "BiOrthogonal 3/1":
          wavelet = new BiOrthogonal31();
          break;

        case "BiOrthogonal 3/3":
          wavelet = new BiOrthogonal33();
          break;

        case "BiOrthogonal 3/5":
          wavelet = new BiOrthogonal35();
          break;

        case "BiOrthogonal 3/7":
          wavelet = new BiOrthogonal37();
          break;

        case "BiOrthogonal 3/9":
          wavelet = new BiOrthogonal39();
          break;

        case "BiOrthogonal 4/4":
          wavelet = new BiOrthogonal44();
          break;

        case "BiOrthogonal 5/5":
          wavelet = new BiOrthogonal55();
          break;

        case "BiOrthogonal 6/8":
          wavelet = new BiOrthogonal68();
          break;

        case "Discrete Mayer":
          wavelet = new DiscreteMayer();
          break;

        case "Battle 23":
          // wavelet = new Battle23( );
          throw new JWaveFailure(
              "WaveletBuilder::create - Battle23 - "
                  + "This wavelet has an odd number of coefficients, "
                  + "due to that it is not comaptible to the implemented algorithms; somehow!");
          // break;

        case "CDF 5/3":
          // wavelet = new CDF53( );
          throw new JWaveFailure(
              "WaveletBuilder::create - CDF 5/3 - "
                  + "This wavelet has an odd number of coefficients, "
                  + "due to that it is not comaptible to the implemented algorithms; somehow!");
          // break;

        case "CDF 9/7":
          // wavelet = new CDF97( );
          throw new JWaveFailure(
              "WaveletBuilder::create - CDF 9/7 - "
                  + "This wavelet has an odd number of coefficients, "
                  + "due to that it is not comaptible to the implemented algorithms; somehow!");
          // break;

        default:
          throw new JWaveFailure(
              "WaveletBuilder::create - unknown type of wavelet for given string!");
      } // switch

    } catch (JWaveException e) {

      e.showMessage();
      e.printStackTrace();
    } // try

    return wavelet;
  } // create

  /**
   * Returns the identifier string of a given Wavelet object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 14:22:22
   * @param wavelet an object of type Wavelet
   * @return identifier string of a given Wavelet object
   */
  public static String identify(Wavelet wavelet) {

    return wavelet.getName();
  } // identify

  /**
   * Create an array keeping all - available - wavelet objects.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 15:39:15
   * @return an array keeping all Wavelet objects
   */
  public static Wavelet[] create2arr() {

    ArrayList<Wavelet> listWavelets = new ArrayList<Wavelet>();

    listWavelets.add(WaveletBuilder.create("Haar"));
    listWavelets.add(WaveletBuilder.create("Daubechies 2"));
    listWavelets.add(WaveletBuilder.create("Daubechies 3"));
    listWavelets.add(WaveletBuilder.create("Daubechies 4"));
    listWavelets.add(WaveletBuilder.create("Daubechies 5"));
    listWavelets.add(WaveletBuilder.create("Daubechies 6"));
    listWavelets.add(WaveletBuilder.create("Daubechies 7"));
    listWavelets.add(WaveletBuilder.create("Daubechies 8"));
    listWavelets.add(WaveletBuilder.create("Daubechies 9"));
    listWavelets.add(WaveletBuilder.create("Daubechies 10"));
    listWavelets.add(WaveletBuilder.create("Daubechies 11"));
    listWavelets.add(WaveletBuilder.create("Daubechies 12"));
    listWavelets.add(WaveletBuilder.create("Daubechies 13"));
    listWavelets.add(WaveletBuilder.create("Daubechies 14"));
    listWavelets.add(WaveletBuilder.create("Daubechies 15"));
    listWavelets.add(WaveletBuilder.create("Daubechies 16"));
    listWavelets.add(WaveletBuilder.create("Daubechies 17"));
    listWavelets.add(WaveletBuilder.create("Daubechies 18"));
    listWavelets.add(WaveletBuilder.create("Daubechies 19"));
    listWavelets.add(WaveletBuilder.create("Daubechies 20"));
    // listWavelets.add( WaveletBuilder.create( "Legendre 1" ) ); // not passing JUnit due to sign
    // listWavelets.add( WaveletBuilder.create( "Legendre 2" ) ); // not passing JUnit due to sign
    // listWavelets.add( WaveletBuilder.create( "Legendre 3" ) ); // not passing JUnit due to sign
    listWavelets.add(WaveletBuilder.create("Coiflet 1"));
    listWavelets.add(WaveletBuilder.create("Coiflet 2"));
    listWavelets.add(WaveletBuilder.create("Coiflet 3"));
    listWavelets.add(WaveletBuilder.create("Coiflet 4"));
    listWavelets.add(WaveletBuilder.create("Coiflet 5"));
    listWavelets.add(WaveletBuilder.create("Symlet 2"));
    listWavelets.add(WaveletBuilder.create("Symlet 3"));
    listWavelets.add(WaveletBuilder.create("Symlet 4"));
    listWavelets.add(WaveletBuilder.create("Symlet 5"));
    listWavelets.add(WaveletBuilder.create("Symlet 6"));
    listWavelets.add(WaveletBuilder.create("Symlet 7"));
    listWavelets.add(WaveletBuilder.create("Symlet 8"));
    listWavelets.add(WaveletBuilder.create("Symlet 9"));
    listWavelets.add(WaveletBuilder.create("Symlet 10"));
    listWavelets.add(WaveletBuilder.create("Symlet 11"));
    listWavelets.add(WaveletBuilder.create("Symlet 12"));
    listWavelets.add(WaveletBuilder.create("Symlet 13"));
    listWavelets.add(WaveletBuilder.create("Symlet 14"));
    listWavelets.add(WaveletBuilder.create("Symlet 15"));
    listWavelets.add(WaveletBuilder.create("Symlet 16"));
    listWavelets.add(WaveletBuilder.create("Symlet 17"));
    listWavelets.add(WaveletBuilder.create("Symlet 18"));
    listWavelets.add(WaveletBuilder.create("Symlet 19"));
    listWavelets.add(WaveletBuilder.create("Symlet 20"));
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 1/1"));
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 1/3"));
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 1/5"));
    // listWavelets.add( WaveletBuilder.create( "BiOrthogonal 2/2" ) ); // not passing JUnit due to
    // shifting one position
    // listWavelets.add( WaveletBuilder.create( "BiOrthogonal 2/4" ) ); // not passing JUnit due to
    // lifting scheme
    // listWavelets.add( WaveletBuilder.create( "BiOrthogonal 2/6" ) ); // not passing JUnit due to
    // lifting scheme
    // listWavelets.add( WaveletBuilder.create( "BiOrthogonal 2/8" ) ); // not passing JUnit due to
    // lifting scheme
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 3/1"));
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 3/3"));
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 3/5"));
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 3/7"));
    listWavelets.add(WaveletBuilder.create("BiOrthogonal 3/9"));
    // listWavelets.add( WaveletBuilder.create( "BiOrthogonal 4/4" ) ); // not passing JUnit due to
    // lifting scheme
    // listWavelets.add( WaveletBuilder.create( "BiOrthogonal 5/5" ) ); // not passing JUnit due to
    // lifting scheme
    // listWavelets.add( WaveletBuilder.create( "BiOrthogonal 6/8" ) ); // not passing JUnit due to
    // lifting scheme
    // listWavelets.add( WaveletBuilder.create( "Discrete Mayer" ) ); // not passing JUnit due to
    // precision

    int noOfWavelets = listWavelets.size();
    Wavelet[] arrWavelets = new Wavelet[noOfWavelets];
    for (int w = 0; w < noOfWavelets; w++) arrWavelets[w] = listWavelets.get(w);

    return arrWavelets;
  } // create2arr
} // class
