package com.parseval;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.parseval.bean.Args;
import com.parseval.exceptions.SchemaParseException;
import com.parseval.exceptions.UnsupportedException;
import com.parseval.exceptions.UserDefineFunctionError;
import com.parseval.planner.ParSevalPlanner;
import com.parseval.planner.ParSevalRelWriter;
import com.parseval.schema.CustomizeSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import py4j.GatewayServer;
import py4j.GatewayServerListener;

import java.io.*;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static String getStackTraceAsString(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();  // Convert StringWriter's buffer to a string
    }

    public String parse(String parameters) {
        List<Map<String, Object>> response = new ArrayList<>();
        Gson gson = new Gson();
        Type typeMyType = new TypeToken<Args>(){}.getType();
        Args args = gson.fromJson(parameters ,typeMyType);

        CustomizeSchema customizeSchema = new CustomizeSchema("default");

        try {
            customizeSchema.create(args.getDdl());
            customizeSchema.addFunctions(args.getFunctions());
        }catch (SchemaParseException e) {
            response.add(Map.of("state", "SCHEMA_ERROR", "error", getStackTraceAsString(e)));
            return gson.toJson(response);
        } catch (UserDefineFunctionError e) {
            response.add(Map.of("state", "USER_DEFINE_FUNCTION_ERROR", "error", getStackTraceAsString(e)));
            return gson.toJson(response);
        }
        ParSevalPlanner planner = new ParSevalPlanner(customizeSchema.plus());
        SqlNode parse = null;
        for (String query : args.getQueries()){
            Map<String, Object> result = new HashMap<>();
            String state = "SUCCESS";
            String help = "";
            String plan = "";
            String error = "";
            long startTime = System.currentTimeMillis();
            try {
                parse = planner.parse(query);
                ParSevalRelWriter parSevalRelWriter = new ParSevalRelWriter();
                planner.rel(parse).explain(parSevalRelWriter);
                help = RelOptUtil.toString(planner.rel(parse), SqlExplainLevel.DIGEST_ATTRIBUTES);
                plan = parSevalRelWriter.asString();
                result.put("error", "");
            } catch (SqlParseException | ValidationException e) {
                state = "SYNTAX_ERROR";
                error = getStackTraceAsString(e);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("********************************************************");
            System.out.println(query);
            if(!help.isBlank()) { System.out.println(help); } else {System.out.println(error); };
            System.out.println("********************************************************");
            response.add(Map.of("state", state, "help", help, "plan", plan, "error", error, "time", endTime - startTime));

            System.out.println("plan" + plan);
        }
        return gson.toJson(response);
    }

    public static String readFile(String filePath) throws IOException {
        StringBuilder contentBuilder = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                contentBuilder.append(currentLine).append("\n"); // Append each line to the StringBuilder
            }
        }
        return contentBuilder.toString(); // Return the full file content as a string
    }

    public static void main(String[] args) throws SchemaParseException, UnsupportedException, IOException, SqlParseException, ValidationException {

        // Main m = new Main();
        // String config = readFile("/home/chunyu/Projects/parser/com.sql.parser/src/main/java/com/parseval/example_input.json");

        // System.out.println(m.parse(config).toString());

       GatewayServer.GatewayServerBuilder builder = new GatewayServer.GatewayServerBuilder(new Main());
       builder.javaAddress(InetAddress.getByName("0.0.0.0"));

       GatewayServer gatewayServer = builder.build();
       gatewayServer.start();
       System.out.println("Gateway Server Started");
    }

}
