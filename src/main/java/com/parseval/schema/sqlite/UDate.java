package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
public class UDate extends SqlFunction{
    public static final UDate INSTANCE = new UDate();
    public UDate() {
        super("UDATE",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.DATE,
                null,
                OperandTypes.STRING,
                SqlFunctionCategory.STRING);
    }
}
