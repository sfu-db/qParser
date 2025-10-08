package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class UDatetime extends SqlFunction{

    public static final UDate INSTANCE = new UDate();
    public UDatetime() {
        super("UDATETIME",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.DATE,
                null,
                OperandTypes.STRING,
                SqlFunctionCategory.STRING);
    }
}
