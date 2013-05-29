package com.ideeli.turmix;

/**
 *
 * @author marc
 */
public class TurmixException extends Exception {
    public TurmixException(String message) {
        super(message);
    }
    public TurmixException(String message,Exception ex) {
        super(message,ex);
    }
}
