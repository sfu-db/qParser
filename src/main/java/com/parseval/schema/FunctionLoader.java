package com.parseval.schema;

import com.google.common.collect.ImmutableMap;
import com.parseval.bean.FunctionDef;
import javassist.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Timestamp;
import  java.util.List;
import java.util.Map;

public class FunctionLoader {
    private static final Map<String, Class<?>> toPrimitive =
            ImmutableMap.<String, Class<?>>builder().put("BINARY", String.class).put("CHAR", String.class)
                    .put("STRING", String.class)
                    .put("VARBINARY", String.class).put("VARCHAR", String.class).put("BLOB", String.class)
                    .put("TINYBLOB", String.class).put("MEDIUMBLOB", String.class).put("LONGBLOB", String.class)
                    .put("TEXT", String.class).put("TINYTEXT", String.class).put("MEDIUMTEXT", String.class)
                    .put("LONGTEXT", String.class).put("ENUM", String.class).put("SET", String.class)
                    .put("BOOL", boolean.class).put("BOOLEAN", boolean.class).put("DEC", double.class)
                    .put("DECIMAL", double.class).put("DOUBLE", double.class).put("DOUBLE PRECISION", double.class)
                    .put("FLOAT", float.class).put("DATE", Date.class).put("DATETIME", Date.class)
                    .put("TIMESTAMP", Timestamp.class).put("TIME", Timestamp.class).put("YEAR", int.class).put("INT", int.class)
                    .put("TINYINT", int.class).put("SMALLINT", int.class).put("MEDIUMINT", int.class)
                    .put("BIGINT", int.class).put("INTEGER", int.class).build();


    ClassPool pool;

    public FunctionLoader() {
        pool  = ClassPool.getDefault();
    }
    public Method load(FunctionDef functionDef) throws RuntimeException, NotFoundException, ClassNotFoundException, NoSuchMethodException {
        CtClass ctClass;
        String className = "com.parseval.schema.CustomizeFunction." + functionDef.hashCode();
        List<String> parameters = functionDef.getParameters();
        Class<?> dynamicClass;

        Class[] classes = new Class[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            classes[i] = toPrimitive.get(parameters.get(i));
        }
        if(pool.getOrNull(className) == null) {
            ctClass = pool.makeClass(className);
            CtClass[] ctClasses = new CtClass[parameters.size()];
            for (int i = 0; i < parameters.size(); i++) {
                ctClasses[i] = pool.get(toPrimitive.get(parameters.get(i)).getName());
            }
            CtClass[] parameterTypes;
            try {
                parameterTypes = convertToCtClassArray(parameters, pool);
                CtClass returnTypeCt = pool.get(toPrimitive.get(functionDef.getReturn_type()).getName());
                CtMethod ctMethod = new CtMethod(returnTypeCt, functionDef.getIdentifier(), parameterTypes, ctClass);
                ctClass.addMethod(ctMethod);
                dynamicClass = ctClass.toClass();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            dynamicClass = Class.forName(className);
        }
        Method method = dynamicClass.getMethod(functionDef.getIdentifier(), classes);
        return method;

    }
//    public Method load(String name, String parameters, String returnType) throws NotFoundException {
//        return load(name, List.of(parameters.split("_")), returnType);
//    }

    private static CtClass[] convertToCtClassArray(List<String> argsList, ClassPool classPool) throws ClassNotFoundException, NotFoundException {
        CtClass[] ctClasses = new CtClass[argsList.size()];
        for (int i = 0; i < argsList.size(); i++) {
            ctClasses[i] = classPool.get(toPrimitive.get(argsList.get(i)).getName());
        }
        return ctClasses;
    }
}
