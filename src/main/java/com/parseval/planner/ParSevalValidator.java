package com.parseval.planner;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class ParSevalValidator extends SqlValidatorImpl {
    /**
     * Creates a validator.
     *
     * @param opTab         Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory   Type factory
     * @param config        Config
     */
    protected ParSevalValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, Config config) {
        super(opTab, catalogReader, typeFactory, config);
    }

    @Override
    protected RelDataType getLogicalSourceRowType(RelDataType sourceRowType, SqlInsert insert) {
        final RelDataType superType = super.getLogicalSourceRowType(sourceRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }
    @Override
    protected RelDataType getLogicalTargetRowType(RelDataType targetRowType, SqlInsert insert) {
        final RelDataType superType = super.getLogicalTargetRowType(targetRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        return super.validate(topNode);
    }
}
