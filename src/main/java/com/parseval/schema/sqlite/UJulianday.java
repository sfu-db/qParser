package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

//JULIANDAY
public class UJulianday extends SqlFunction {
    public static final UJulianday INSTANCE = new UJulianday();
    public UJulianday() {
        super("JULIANDAY",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER,
                null,
                OperandTypes.DATE_OR_TIMESTAMP,
                SqlFunctionCategory.TIMEDATE);
    }

}

