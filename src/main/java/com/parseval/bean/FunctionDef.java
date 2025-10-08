package com.parseval.bean;

import java.util.List;
import java.util.Objects;

public class FunctionDef {
    String type;
    String identifier;
    List<String> parameters;
    String return_type;

    public List<String> getParameters() {
        return parameters;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getReturn_type() {
        return return_type;
    }

    public String getType() {
        return type;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void setParameters(List<String> parameters) {
        this.parameters = parameters;
    }

    public void setReturn_type(String return_type) {
        this.return_type = return_type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "FunctionDef{" +
                "type='" + type + '\'' +
                ", identifier='" + identifier + '\'' +
                ", parameters=" + parameters +
                ", returnType='" + return_type + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionDef that = (FunctionDef) o;
        return Objects.equals(type, that.type) && Objects.equals(identifier, that.identifier) && Objects.equals(parameters, that.parameters) && Objects.equals(return_type, that.return_type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, identifier, parameters, return_type);
    }
}
