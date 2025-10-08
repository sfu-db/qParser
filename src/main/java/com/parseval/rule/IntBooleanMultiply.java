package com.parseval.rule;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Arrays;

public class IntBooleanMultiply extends SqlBinaryOperator {
    public IntBooleanMultiply() {
        super("*", SqlKind.TIMES, 40, true, ReturnTypes.INTEGER, null, null);
    }


    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
        // Here we can convert BOOLEAN to INTEGER (TRUE -> 1, FALSE -> 0) before multiplying
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);

        SqlTypeName leftType = validator.getValidatedNodeType(left).getSqlTypeName();
        SqlTypeName rightType = validator.getValidatedNodeType(right).getSqlTypeName();

        if (rightType == SqlTypeName.BOOLEAN) {

            SqlNode thenResult = SqlLiteral.createExactNumeric("1", call.getParserPosition()); // TRUE -> 1
            SqlNode elseResult = SqlLiteral.createExactNumeric("0", call.getParserPosition()); // FALSE -> 0
            SqlNode caseExpression = new SqlCase(
                    call.getParserPosition(),                      // Parser position
                    null,                                          // No SWITCH expression
                    new SqlNodeList(Arrays.asList(right), call.getParserPosition()), // WHEN list
                    new SqlNodeList(Arrays.asList(thenResult), call.getParserPosition()),    // THEN list
                    elseResult                                     // ELSE result
            );

            return SqlStdOperatorTable.MULTIPLY.createCall(call.getParserPosition(), left, caseExpression);

        }

        // Return the new multiplication expression: INTEGER * (BOOLEAN converted to INTEGER)
        return call;
    }
}
