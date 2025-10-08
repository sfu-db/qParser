package com.parseval.schema.mysql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class UDateSub  extends SqlFunction{
    public static final UDateSub INSTANCE = new UDateSub();
    public UDateSub() {
        super("DATE_SUB",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.DATE,
                null,
                OperandTypes.DATETIME_INTERVAL,
                SqlFunctionCategory.TIMEDATE);
    }
}
