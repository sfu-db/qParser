package com.parseval.bean;

import java.util.List;
import java.util.Objects;

public class Args {
    private List<String> ddl;
    private List<String> queries;
    private List<FunctionDef> functions;

    public List<FunctionDef> getFunctions() {
        return functions;
    }

    public List<String> getDdl() {
        return ddl;
    }

    public List<String> getQueries() {
        return queries;
    }

    public void setDdl(List<String> ddl) {
        this.ddl = ddl;
    }

    public void setFunctions(List<FunctionDef> functions) {
        this.functions = functions;
    }

    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

}
