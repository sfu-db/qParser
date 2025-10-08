package com.parseval.exceptions;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SchemaParseException extends Exception{

    public SchemaParseException() {
        super("Error parsing the schema.");
    }

    // Constructor that accepts a custom message
    public SchemaParseException(String message) {
        super(message);
    }

    // Constructor that accepts a message and a cause (underlying exception)
    public SchemaParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchemaParseException(Throwable cause) {
        super("Schema Error", cause);
    }
}
