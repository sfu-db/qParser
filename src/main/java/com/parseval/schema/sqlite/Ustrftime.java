package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class Ustrftime extends SqlFunction {
    public static final Ustrftime INSTANCE = new Ustrftime();
    public Ustrftime() {
        super("STRFTIME",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR,
                null,
                OperandTypes.ANY_ANY,
                SqlFunctionCategory.TIMEDATE);
    }

}
