package com.parseval.schema.sqlite;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

public class ConditionVisitor extends RexVisitorImpl<RexNode> {

    protected ConditionVisitor(boolean deep) {
        super(deep);
    }



    @Override
    public RexNode visitCall(RexCall call) {
        if(call.isA(SqlKind.FILTER)){
//            call.getOperands().get(1)
        }

        return super.visitCall(call);
    }
}
