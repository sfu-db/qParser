package com.parseval.exceptions;

public class SyntaxError  extends Exception{

    public SyntaxError() {
        super("Error when parsing the query.");
    }

    // Constructor that accepts a custom message
    public SyntaxError(String message) {
        super(message);
    }

    // Constructor that accepts a message and a cause (underlying exception)
    public SyntaxError(String message, Throwable cause) {
        super(message, cause);
    }

    public SyntaxError(Throwable cause) {
        super("Syntax Error", cause);
    }
}
