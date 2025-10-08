package com.parseval.schema;

import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

import kala.collection.Map;

//import java.util.*;

public class CustomizeTable extends AbstractTable {
    String name;
    Seq<String> columnNames;
    Seq<RelDataType> columnTypes;
    Set<ImmutableBitSet> keys;
    Set<RexNode> constraints;


    public CustomizeTable(String name, Seq<String> columnNames, Seq<RelDataType> columnTypes, Set<ImmutableBitSet> keys, Set<RexNode> constraints) {
        this.name = name;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.keys = keys;
        this.constraints = constraints;
    }

    public CustomizeTable(String identifier, Map<String, RelDataType> columns, ImmutableSet<ImmutableBitSet> eligibleKeys,
                          Set<RexNode> checkConstraints) {
        name = identifier;
        columnNames = columns.keysView().toImmutableSeq().sorted();
        columnTypes = columnNames.map(columns::get);
        keys = eligibleKeys;
        constraints = checkConstraints;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(columnNames.zip(columnTypes).view().map(entry -> java.util.Map.entry(entry.component1(), entry.component2())).toImmutableSeq().asJava());
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(0,keys.toImmutableSeq().asJava());
    }

    public Seq<RelDataType> getColumnTypes() {
        return columnTypes;
    }

    public Seq<String> getColumnNames() {
        return columnNames;
    }

    public String getName() {
        return name;
    }

    public Set<ImmutableBitSet> getKeys() {
        return keys;
    }

    public Set<RexNode> getConstraints() {
        return constraints;
    }
}
