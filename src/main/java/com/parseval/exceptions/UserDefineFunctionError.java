package com.parseval.exceptions;

public class UserDefineFunctionError extends Exception{
    public UserDefineFunctionError() {
        super("Error when creating the function.");
    }

    // Constructor that accepts a custom message
    public UserDefineFunctionError(String message) {
        super(message);
    }

    // Constructor that accepts a message and a cause (underlying exception)
    public UserDefineFunctionError(String message, Throwable cause) {
        super(message, cause);
    }

    public UserDefineFunctionError(Throwable cause) {
        super("User Defined Funciont Error", cause);
    }

}
