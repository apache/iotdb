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
package org.apache.iotdb.db.query.simpiece.jwave.exceptions;

/**
 * General exception class of the JWave package.
 *
 * @date 16.10.2008 07:30:20
 * @author Christian (graetz23@gmail.com)
 */
public class JWaveException extends Throwable {

  /**
   * @date 27.05.2009 06:58:27
   * @author Christian (graetz23@gmail.com)
   */
  private static final long serialVersionUID = -4165486739091019056L;

  /** Member string for the stored exception message. */
  protected String _message; // exception message

  /**
   * Constructor for storing a handed exception message.
   *
   * @date 27.05.2009 06:51:57
   * @author Christian (graetz23@gmail.com)
   * @param message this message should tell exactly what went wrong
   */
  public JWaveException(String message) {
    _message = "JWave "; // overwrite
    _message += "- "; // separator
    _message += "Exception"; // Exception type
    _message += "- "; // separator
    _message += message; // add message
    _message += "\n"; // break line
  } // TransformException

  /**
   * Copy constructor; use this for a quick fix of sub types.
   *
   * @date 29.07.2009 07:03:45
   * @author Christian (graetz23@gmail.com)
   * @param e an object of this class
   */
  public JWaveException(Exception e) {
    _message = e.getMessage();
  } // TransformException

  /**
   * Returns the stored exception message as a string.
   *
   * @date 27.05.2009 06:52:46
   * @author Christian (graetz23@gmail.com)
   * @return exception message that should tell exactly what went wrong
   */
  @Override
  public String getMessage() {
    return _message;
  } // getMessage

  /**
   * Displays the stored exception message at console out.
   *
   * @date 27.05.2009 06:53:23
   * @author Christian (graetz23@gmail.com)
   */
  public void showMessage() {
    System.out.println(_message);
  } // showMessage

  /**
   * Nuke the run and print stack trace.
   *
   * @date 02.07.2009 05:07:42
   * @author Christian (graetz23@gmail.com)
   */
  public void nuke() {
    System.out.println("");
    System.out.println("                  ____             ");
    System.out.println("          __,-~~/~    `---.        ");
    System.out.println("        _/_,---(      ,    )       ");
    System.out.println("    __ /        NUKED     ) \\ __  ");
    System.out.println("   ====------------------===;;;==  ");
    System.out.println("      /  ~\"~\"~\"~\"~\"~~\"~)     ");
    System.out.println("      (_ (      (     >    \\)     ");
    System.out.println("       \\_( _ <         >_>\'      ");
    System.out.println("           ~ `-i' ::>|--\"         ");
    System.out.println("               I;|.|.|             ");
    System.out.println("              <|i::|i|>            ");
    System.out.println("               |[::|.|             ");
    System.out.println("                ||: |              ");
    System.out.println("");
    this.showMessage();
    this.printStackTrace();
  } // nuke
} // class
