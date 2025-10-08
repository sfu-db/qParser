package com.parseval.exceptions;

public class UnsupportedException extends RuntimeException{

    // Default constructor
    public UnsupportedException() {
        super("Operation is not supported.");
    }

    // Constructor that accepts a custom message
    public UnsupportedException(String message) {
        super(message);
    }
    // Constructor that accepts a message and a cause
    public UnsupportedException(String message, Throwable cause) {
        super(message, cause);
    }

}
