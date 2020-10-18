package org.wisekg.util;

import com.google.gson.*;
import org.wisekg.executionplan.QueryExecutionPlan;
import org.wisekg.executionplan.QueryOperator;
import org.rdfhdt.hdt.util.StarString;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class PlanDeserializer implements JsonDeserializer<QueryExecutionPlan> {
    @Override
    public QueryExecutionPlan deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        return deserializeSubplan(jsonElement);
    }

    private QueryExecutionPlan deserializeSubplan(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        if(!obj.has("operator") || !obj.has("subplan"))
            return QueryExecutionPlan.getNullPlan();

        return new QueryExecutionPlan(deserializeOperator(obj.get("operator")), deserializeSubplan(obj.get("subplan")), Long.parseLong(obj.get("timestamp").getAsString()));
    }

    private QueryOperator deserializeOperator(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        String control = obj.get("control").getAsString();

        return new QueryOperator(control, deserializeStar(obj.get("star")));
    }

    private StarString deserializeStar(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        String subject = obj.get("subject").getAsString();

        return new StarString(subject, deserializeTriples(obj.getAsJsonArray("triples")));
    }

    private List<Tuple<CharSequence, CharSequence>> deserializeTriples(JsonArray arr) {
        List<Tuple<CharSequence, CharSequence>> lst = new ArrayList<>();

        int size = arr.size();
        for(int i = 0; i < size; i++) {
            lst.add(deserializeTriple(arr.get(i)));
        }

        return lst;
    }

    private Tuple<CharSequence, CharSequence> deserializeTriple(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        return new Tuple<>(obj.get("first").getAsString(), obj.get("second").getAsString());
    }
}
