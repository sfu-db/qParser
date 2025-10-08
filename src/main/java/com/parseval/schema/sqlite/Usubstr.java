package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;


public class Usubstr extends SqlFunction{
    public static final Usubstr INSTANCE = new Usubstr();
    public Usubstr() {
        super("SUBSTR",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR,
                null,
                OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
                SqlFunctionCategory.STRING);
    }
}
