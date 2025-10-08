package com.parseval.schema.sqlite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class UInstr extends  SqlFunction{
    public static final UInstr INSTANCE = new UInstr();
    public UInstr() {
        super("INSTR",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER,
                null,
                OperandTypes.STRING_STRING,
                SqlFunctionCategory.STRING);
    }
}
