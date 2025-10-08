package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
public class ULength extends SqlFunction{
    public static final ULength INSTANCE = new ULength();
    public ULength() {
        super("LENGTH",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER,
                null,
                OperandTypes.ANY,
                SqlFunctionCategory.STRING);
    }
}
