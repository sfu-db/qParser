package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
public class UNowFunction extends SqlFunction {
    public static final UNowFunction INSTANCE = new UNowFunction();
    public UNowFunction() {
        super("NOW", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP, null, OperandTypes.NILADIC, SqlFunctionCategory.TIMEDATE);
    }
}