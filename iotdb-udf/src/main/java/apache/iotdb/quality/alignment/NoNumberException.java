/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package apache.iotdb.quality.alignment;

public class NoNumberException extends Exception {

    @Override
    public String toString() {
        String s = "The value of the input time series is not numeric.\n";
        return s + super.toString(); //To change body of generated methods, choose Tools | Templates.
    }

}
