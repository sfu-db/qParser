package com.parseval.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import kala.collection.Seq;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.calcite.rel.logical.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

import org.apache.calcite.rel.type.RelDataTypeField;

public class ParSevalRelWriter implements RelWriter {

    private static final Map<String, Class<?>> toPrimitive =
            ImmutableMap.<String, Class<?>>builder().put("BINARY", String.class).put("CHAR", String.class)
                    .put("VARBINARY", String.class).put("VARCHAR", String.class).put("BLOB", String.class)
                    .put("TINYBLOB", String.class).put("MEDIUMBLOB", String.class).put("LONGBLOB", String.class)
                    .put("TEXT", String.class).put("TINYTEXT", String.class).put("MEDIUMTEXT", String.class)
                    .put("LONGTEXT", String.class).put("ENUM", String.class).put("SET", String.class)
                    .put("BOOL", Boolean.class).put("BOOLEAN", Boolean.class).put("DEC", Double.class)
                    .put("DECIMAL", Double.class).put("DOUBLE", Double.class).put("DOUBLE PRECISION", Double.class)
                    .put("FLOAT", Float.class).put("DATE", Date.class).put("DATETIME", Timestamp.class)
                    .put("TIMESTAMP", Timestamp.class).put("TIME", Time.class).put("YEAR", int.class).put("INT", Integer.class)
                    .put("TINYINT", Integer.class).put("SMALLINT", Integer.class).put("MEDIUMINT", Integer.class)
                    .put("BIGINT", Integer.class).put("INTEGER", Integer.class).build();

    protected final JsonBuilder jsonBuilder;
    protected final RelJson relJson;
    private final IdentityHashMap<RelNode, String> relIdMap = new IdentityHashMap<>();
    protected final List<@Nullable Object> relList;
    protected final Map<String, Object> relMap;

    private final List<Pair<String, @Nullable Object>> values = new ArrayList<>();
    private @Nullable String previousId;

    public ParSevalRelWriter() {
        this(new JsonBuilder());
    }

    public ParSevalRelWriter(JsonBuilder jsonBuilder) {
        this(jsonBuilder, UnaryOperator.identity());
    }

    public ParSevalRelWriter(JsonBuilder jsonBuilder,
                             UnaryOperator<RelJson> relJsonTransform) {
        this.jsonBuilder = requireNonNull(jsonBuilder, "jsonBuilder");
        relList = this.jsonBuilder.list();
        relMap = this.jsonBuilder.map();
        relJson = relJsonTransform.apply(RelJson.create().withJsonBuilder(jsonBuilder));
    }


    protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
        final Map<String, @Nullable Object> map = jsonBuilder.map();
        map.put("relOp", relJson.classToTypeName(rel.getClass()));
        String relOp = relJson.classToTypeName(rel.getClass());

        switch (relOp) {
            case "LogicalTableScan" -> {
                var qName = rel.getTable().getQualifiedName();
                put(map, "table", qName.get(qName.size() - 1));

                var node = (LogicalTableScan) rel;

                final List<Object> columns = jsonBuilder.list();

                for (RelDataTypeField field :rel.getRowType().getFieldList()) {
                    final Map<String, @Nullable Object> column = jsonBuilder.map();
                    column.put("name", field.getName());
                    column.put("type", serialize(field.getType()));
                    columns.add(column);
                }
                map.put("columns", columns);

            }
            case "LogicalValues" -> {
                var node = (LogicalValues) rel;
                List<Object> vals = jsonBuilder.list();
                for (ImmutableList tuples: node.getTuples()) {
                    for(Object tuple : tuples){
                        vals.add(serialize((RexNode)tuple));
                    }
                }
                map.put("values", vals);
            }
            case "LogicalFilter" -> {
                var filter = (LogicalFilter) rel;
                map.put("condition", serialize(filter.getCondition()));
                map.put("variableset", filter.getVariablesSet().toString());
            }
            case "LogicalProject" -> {
                var node = (LogicalProject) rel;
                List<Object> targets = jsonBuilder.list();
                for(RexNode project : node.getProjects()) {
                    targets.add(serialize(project));
                }
                map.put("project", targets);
            }
            case "LogicalJoin" -> {
                var join = (LogicalJoin) rel;

                map.put("joinType", join.getJoinType().lowerName);
                map.put("condition", serialize(join.getCondition()));
            }
            case "LogicalAggregate" -> {
                var aggregate = (LogicalAggregate) rel;
                var input = aggregate.getInput();
                var inputTypes = Seq.from(input.getRowType().getFieldList()).map(field -> type(field.getType()));
                var groupSet = jsonBuilder.list();
                var aggFunc = jsonBuilder.list();
                for (Iterator<Integer> it = aggregate.getGroupSet().iterator(); it.hasNext(); ) {
                    Integer key = it.next();
                    var keys = jsonBuilder.map();
                    keys.put("column", key);
                    keys.put("type", inputTypes.get(key));
                    groupSet.add(keys);
                }
                for(AggregateCall aggCall :aggregate.getAggCallList()) {
                    var aggMap = jsonBuilder.map();
                    aggMap.put("operator", aggCall.getAggregation().getName());
                    if (aggCall.getAggregation().getFunctionType().isUserDefined()) {
                        aggMap.put("operator", aggCall.getAggregation().getClass().getName());
                    }
                    aggMap.put("distinct", aggCall.isDistinct());
                    var args = jsonBuilder.list();
                    for(Integer arg :aggCall.getArgList()) {
                        var t = jsonBuilder.map();
                        t.put("column", arg);
                        t.put("type", inputTypes.get(arg));
                        args.add(t);
                    }


                    aggMap.put("distinct", aggCall.isDistinct());
                    aggMap.put("ignoreNulls", aggCall.ignoreNulls());
                    aggMap.put("operands", args);
                    aggMap.put("type", type(aggCall.getType()));
                    aggMap.put("name", aggCall.getName());
                    aggFunc.add(aggMap);
                }
                map.put("keys", groupSet);
                map.put("aggs", aggFunc);


            }
            case "LogicalCorrelate" -> {
                var correlate = (LogicalCorrelate) rel;
                map.put("kind", correlate.getJoinType().toString());
            }
            case "LogicalUnion" -> {
                var union = (LogicalUnion)rel;
                map.put("all", union.all);
            }
            case "LogicalIntersect" -> {
                var intersect = (LogicalIntersect)rel;
                map.put("all", intersect.all);
            }
            case "LogicalMinus" -> {
                var except = (LogicalMinus)rel;
                map.put("all", except.all);
            }
            case "LogicalSort" -> {
                var sort = (LogicalSort)rel;
                var input = sort.getInput();
                var types = Seq.from(input.getRowType().getFieldList()).map(field -> type(field.getType()));
                var collations = jsonBuilder.list();
                var directions = jsonBuilder.list();
                for(RelFieldCollation collation : sort.collation.getFieldCollations()) {
                    int index = collation.getFieldIndex();
                    var mm = jsonBuilder.map();
                    mm.put("column", index);
                    mm.put("type", types.get(index));
                    collations.add(mm);
                    directions.add(collation.getDirection().name());
                }
                map.put("sort", collations);
                map.put("dir", directions);
                map.put("offset", sort.offset != null? RexLiteral.intValue(sort.offset): 0);
                map.put("limit", sort.fetch != null? RexLiteral.intValue(sort.fetch) : null);
            }

            default -> throw new IllegalStateException("Unexpected value: " + relOp);
        }
        final List<@Nullable Object> list = explainInputs(rel.getInputs());
        final String id = Integer.toString(relIdMap.size());


        relIdMap.put(rel, id);
        relList.add(map);
        relMap.put(id, map);
        List<Object> ll = new ArrayList<>();
        for (Object rid : list) {
            ll.add(relMap.get(rid));
        }
        map.put("id", id);
        map.put("inputs", ll);
        previousId = id;
    }
    private Map<String, @Nullable Object> serialize(RelDataType relDataType) {
        Map<String, @Nullable Object> map = jsonBuilder.map();
        put(map, "name", relDataType.getSqlTypeName().getName());
        put(map, "nullable", relDataType.isNullable());
        put(map, "precision", relDataType.getPrecision());
        put(map, "scale", relDataType.getScale());
        return  map;
    }
    private Map<String, @Nullable Object> serialize(RexNode rex) {
        Map<String, @Nullable Object> map = jsonBuilder.map();
        map.put("kind", rex.getKind().toString());
        map.put("type", serialize(rex.getType()));

        if(rex instanceof RexInputRef) {
            var inputRef = (RexInputRef)rex;
            map.put("index", inputRef.getIndex());
            map.put("name", inputRef.getName());

        }else if (rex instanceof RexLiteral) {
            var literal = (RexLiteral) rex;
            Object operator = literal.getValue() == null ? "NULL" : getLiteralValue(literal);
            put(map, "value", operator);

        } else if (rex instanceof  RexSubQuery) {
            var rexSubQuery = (RexSubQuery) rex;
            map.put("operator", rexSubQuery.getOperator().getName());
            List operands = jsonBuilder.list();
            for(RexNode operand : rexSubQuery.getOperands()) {
                operands.add(serialize(operand));
            }
            map.put("operands", operands);
            List<Object> queryList = jsonBuilder.list();
            for (Object rid : explainInputs(List.of(rexSubQuery.rel))) {
                queryList.add(relMap.get(rid));
            }

            map.put("query", queryList);
        } else if(rex instanceof RexCall) {
            var rexCall = (RexCall) rex;
            String operator = rexCall.getOperator().getName();

            map.put("operator", operator);
            List operands = jsonBuilder.list();
            for(RexNode operand : rexCall.getOperands()) {
                operands.add(serialize(operand));
            }
            map.put("operands", operands);
        } else if (rex instanceof  RexFieldAccess) {
            var fieldAccess = (RexFieldAccess) rex;
            CorrelationId id = ((RexCorrelVariable) fieldAccess.getReferenceExpr()).id;
            map.put("column", fieldAccess.getField().getIndex());
            map.put("name", id.toString());
        } else {
            throw  new RuntimeException("Not Implemented: " + rex.getKind());
        }


        return  map;
    }

    private static String type(RelDataType relDataType){


        return type(relDataType.getSqlTypeName());
    }

    private static String type(SqlTypeName sqlTypeName){



        return sqlTypeName.getName();
    }

    private static Object getLiteralValue(RexLiteral rexLiteral) {
        SqlTypeName typeName = rexLiteral.getType().getSqlTypeName();
        return rexLiteral.getValueAs(toPrimitive.get(typeName.getName()));
    }

    private List<@Nullable Object> explainInputs(List<RelNode> inputs) {
        final List<@Nullable Object> list = jsonBuilder.list();
        for (RelNode input : inputs) {
            String id = relIdMap.get(input);
            if (id == null) {
                input.explain(this);
                id = previousId;
            }
            list.add(id);
        }
        return list;
    }


    @Override
    public void explain(RelNode rel, List<Pair<String, @Nullable Object>> valueList) {
        explain_(rel, valueList);

    }

    private void put(Map<String, @Nullable Object> map, String name, @Nullable Object value) {
        map.put(name, relJson.toJson(value));
    }

    @Override
    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }

    @Override
    public RelWriter item(String term, @Nullable Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    @Override
    public RelWriter done(RelNode node) {
        final List<Pair<String, @Nullable Object>> valuesCopy =
                ImmutableList.copyOf(values);
        values.clear();
        explain_(node, valuesCopy);
        return this;
    }

    public String asString() {
        final Map<String, @Nullable Object> map = jsonBuilder.map();
        map.put("rels", relList);
//        System.out.println("previousId is " + previousId);
        return jsonBuilder.toJsonString(relMap.get(previousId));
    }

}
    //
    //

    /**
     *  relOp: {
     *      input: {},
     *      condition: {}
     *      operand: [{
     *          column: idx,
     *          type:
     *          }
     *      ],
     *      operator:
     *  }

     */

