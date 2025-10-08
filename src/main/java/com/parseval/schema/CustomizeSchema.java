package com.parseval.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.parseval.bean.Args;
import com.parseval.bean.FunctionDef;
import com.parseval.exceptions.SchemaParseException;
import com.parseval.exceptions.UnsupportedException;
import com.parseval.exceptions.UserDefineFunctionError;
import com.parseval.rule.IntBooleanMultiply;
import javassist.NotFoundException;
import kala.collection.Seq;
import kala.collection.immutable.ImmutableSeq;
import kala.collection.immutable.ImmutableSet;
import kala.collection.mutable.MutableHashMap;
import kala.collection.mutable.MutableMap;
import kala.collection.mutable.MutableList;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCheckConstraint;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;

import org.apache.calcite.util.ImmutableBitSet;

import java.io.Serializable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.calcite.sql.type.SqlTypeName;
import org.example.sql.operands.CustomOperand;

public class CustomizeSchema extends AbstractSchema implements Serializable{
    private String name;
    MutableMap<String, Table> tables;

    private FunctionLoader functionLoader;

    private final Map<String, List<Function>> declaredFunctions = new HashMap<>();

    private static final Map<String, Class<?>> toPrimitive =
            ImmutableMap.<String, Class<?>>builder().put("BINARY", String.class).put("CHAR", String.class)
                    .put("VARBINARY", String.class).put("VARCHAR", String.class).put("BLOB", String.class)
                    .put("TINYBLOB", String.class).put("MEDIUMBLOB", String.class).put("LONGBLOB", String.class)
                    .put("TEXT", String.class).put("TINYTEXT", String.class).put("MEDIUMTEXT", String.class)
                    .put("LONGTEXT", String.class).put("ENUM", String.class).put("SET", String.class)
                    .put("BOOL", boolean.class).put("BOOLEAN", boolean.class).put("DEC", double.class)
                    .put("DECIMAL", double.class).put("DOUBLE", double.class).put("DOUBLE PRECISION", double.class)
                    .put("FLOAT", float.class).put("DATE", int.class).put("DATETIME", int.class)
                    .put("TIMESTAMP", int.class).put("TIME", int.class).put("YEAR", int.class).put("INT", int.class)
                    .put("TINYINT", int.class).put("SMALLINT", int.class).put("MEDIUMINT", int.class)
                    .put("BIGINT", int.class).put("INTEGER", int.class).build();

    private static final Map<String, SqlTypeName> typeNormalizing =
            ImmutableMap.<String, SqlTypeName>builder().put("BINARY", SqlTypeName.VARCHAR).put("CHAR", SqlTypeName.VARCHAR)
                    .put("VARBINARY",  SqlTypeName.VARCHAR).put("VARCHAR",  SqlTypeName.VARCHAR).put("BLOB",  SqlTypeName.VARCHAR)
                    .put("TINYBLOB",  SqlTypeName.VARCHAR).put("MEDIUMBLOB",  SqlTypeName.VARCHAR).put("LONGBLOB",  SqlTypeName.VARCHAR)
                    .put("TEXT",  SqlTypeName.VARCHAR).put("TINYTEXT",  SqlTypeName.VARCHAR).put("MEDIUMTEXT",  SqlTypeName.VARCHAR)
                    .put("LONGTEXT",  SqlTypeName.VARCHAR).put("ENUM",  SqlTypeName.VARCHAR).put("SET",  SqlTypeName.VARCHAR)
                    .put("STRING", SqlTypeName.VARCHAR)
                    .put("BOOL",  SqlTypeName.BOOLEAN).put("BOOLEAN",  SqlTypeName.BOOLEAN).put("DEC",  SqlTypeName.FLOAT)
                    .put("DECIMAL",  SqlTypeName.FLOAT).put("DOUBLE",  SqlTypeName.FLOAT).put("DOUBLE PRECISION",  SqlTypeName.FLOAT)
                    .put("FLOAT",  SqlTypeName.FLOAT).put("DATE", SqlTypeName.DATE).put("DATETIME", SqlTypeName.DATE)
                    .put("TIMESTAMP", SqlTypeName.TIMESTAMP).put("TIME", SqlTypeName.TIME).put("YEAR", SqlTypeName.INTEGER)
                    .put("INT", SqlTypeName.INTEGER)
                    .put("TINYINT", SqlTypeName.INTEGER).put("SMALLINT", SqlTypeName.INTEGER).put("MEDIUMINT", SqlTypeName.INTEGER)
                    .put("BIGINT", SqlTypeName.INTEGER).put("INTEGER", SqlTypeName.INTEGER).put("OTHERS", SqlTypeName.BOOLEAN).build();

    private static final SqlParser.Config schemaParserConfig =
            SqlParser.Config.DEFAULT
                    .withParserFactory(SqlDdlParserImpl.FACTORY)
                    .withLex(Lex.MYSQL_ANSI)
                    .withCaseSensitive(false)
                    .withQuoting(Quoting.DOUBLE_QUOTE);

    public CustomizeSchema(String name){
        this.name = name;
        tables  = new MutableHashMap<>();
        functionLoader = new FunctionLoader();
    }

    public void create(List<String> ddl) throws SchemaParseException, UnsupportedException {
        for (String query : ddl){
            query = query.replace("`", "\"");
            Pattern supported = Pattern.compile("(?i)CREATE\\s+(VIEW|TABLE)");
            if (!supported.matcher(query).find()) {
                throw new SchemaParseException("Unexpected value: " + query);
            }
            SqlParser schemaParser = SqlParser.create(query, schemaParserConfig);
            SqlCreateTable schemaNode = null;
            try {
                schemaNode = (SqlCreateTable)schemaParser.parseStmt();
                addTable(schemaNode);
            } catch (SqlParseException e) {
                throw new SchemaParseException(e);
            }
        }
    }
    public void create(String ddls) throws SchemaParseException, UnsupportedException {
        String[] queries = ddls.split(";");
        create(List.of(queries));
    }
    private void addTable(SqlCreateTable node) throws UnsupportedException{
        assert node instanceof SqlCreateTable;
        var names = MutableList.<String>create();
        var types = MutableList.<SqlTypeName>create();
        var nullabilities = MutableList.<Boolean>create();
        var keys = MutableList.<ImmutableBitSet>create();
        var checkConstraints = MutableList.<RexNode>create();

        for (SqlNode column : node.columnList) {
            switch (column.getKind()){
                case COLUMN_DECL -> {
                    SqlColumnDeclaration decl = (SqlColumnDeclaration) column;
                    names.append(decl.name.toString());
                    SqlTypeName typ =SqlTypeName.get(decl.dataType.getTypeName().toString());
                    if(typ == null) {
                        typ = typeNormalizing.getOrDefault(decl.dataType.getTypeName().toString().toUpperCase(), SqlTypeName.ANY);
                    }
                    types.append(typ);
                    nullabilities.append(decl.strategy != ColumnStrategy.NOT_NULLABLE);
                }
                case FOREIGN_KEY, CHECK -> System.err.println("Foreign key/CHECK constraints are not implemented yet.");
                case PRIMARY_KEY, UNIQUE -> {
                    SqlKeyConstraint cons = (SqlKeyConstraint) column;
                    List<Integer> key = new ArrayList<>();
                    for (SqlNode id : (SqlNodeList) cons.getOperandList().get(1)) {

                        int index = names.indexOf(id.toString());
                        key.add(index);
                        if (column.getKind() == SqlKind.PRIMARY_KEY) {
                            nullabilities.set(index, false);
                        }
                    }
                    keys.append(ImmutableBitSet.of(key));
                }
                default -> throw new UnsupportedException(
                        "Unsupported declaration type " + column.getKind() + " in table " + node.name);
            }
        }
        var customizeTable = new CustomizeTable(node.name.toString(), names.toImmutableSeq(),
                ImmutableSeq.from(types.zip(nullabilities).map(type -> new RelType.BaseType(type.component1(), type.component2()))),
                ImmutableSet.from(keys), ImmutableSet.from(checkConstraints));
        tables.put(node.name.toString(), customizeTable);
    }

    public void addFunctions(List<FunctionDef> funcDefines) throws UserDefineFunctionError {
        for (FunctionDef funcDef : funcDefines) {
            addFunction(funcDef);
        }
    }

    public void addFunction(FunctionDef functionDef) throws UnsupportedException, UserDefineFunctionError {
        Function customFunction = null;
        switch (functionDef.getType().toUpperCase()) {
            case "SCALAR" -> {
                Method scalarFunction = null;
                try {
                    scalarFunction = functionLoader.load(functionDef);
                    customFunction = ScalarFunctionImpl.createUnsafe(scalarFunction);
                } catch (NotFoundException | ClassNotFoundException | NoSuchMethodException e) {
                    throw new UserDefineFunctionError("Cannot initialize SCALAR function " + functionDef.getIdentifier(), e);
                }
            }
            case "AGGREGATE" -> {
//                    ReflectiveFunctionBase.ParameterListBuilder sourceParameters = ReflectiveFunctionBase.builder();
//                    ImmutableList.Builder<Class<?>> sourceTypes = ImmutableList.builder();
//                    for (Class<?> clazz : parameters) {
//                        sourceParameters.add(clazz, clazz.getName(), false);
//                        sourceTypes.add(clazz);
//                    }
//                    Method nullFunction = methodConstructor.newInstance(CustomizeSchema.class, "qedFunction", parameters,
//                            toPrimitive.get(returnType), null, 0, 0, "", null, null, null);
//                    Constructor<AggregateFunctionImpl> aggregateFunctionConstructor =
//                            AggregateFunctionImpl.class.getDeclaredConstructor(Class.class, List.class, List.class, Class.class,
//                                    Class.class, Method.class, Method.class, Method.class, Method.class);
//                    aggregateFunctionConstructor.setAccessible(true);
//                    customFunction = aggregateFunctionConstructor.newInstance(CustomizeSchema.class, sourceParameters.build(),
//                            sourceTypes.build(), toPrimitive.get(returnType), toPrimitive.get(returnType), nullFunction, nullFunction,
//                            null, null);
            }
            default -> throw new UnsupportedException("Unsupported function " + functionDef.getIdentifier());
        }
        if(! declaredFunctions.containsKey(functionDef.getIdentifier())) {
            declaredFunctions.put(functionDef.getIdentifier(), new ArrayList<Function>());
        }
        declaredFunctions.get(functionDef.getIdentifier()).add(customFunction);
//        declaredFunctions.put(functionDef.getIdentifier(), customFunction);

    }
//    public void addFunction(String funcType, String identifier,String operandType, String returnType) throws UnsupportedException {
//        String[] operandsType =  operandType.split("_");
//        addFunction(funcType, identifier, List.of(operandsType), returnType);
//    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tables.asJava();
    }


    public String getName() {
        return name;
    }

    public SchemaPlus plus() {
        SchemaPlus schemaPlus = CalciteSchema.createRootSchema(true, false, "ParSeval", this).plus();
        for (String fn : declaredFunctions.keySet()) {
            for (Function func : declaredFunctions.get(fn)) {
                schemaPlus.add(fn, func);
            }
        }
        return schemaPlus;
    }
}
