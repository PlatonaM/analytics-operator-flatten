package util;

import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Deserializer {
    private static Number parse(String str) {
        Number number;
        try {
            number = Integer.parseInt(str);
        } catch (NumberFormatException e) {
            try {
                number = Long.parseLong(str);
            } catch (NumberFormatException e1) {
                try {
                    number = Float.parseFloat(str);
                } catch (NumberFormatException e2) {
                    try {
                        number = Double.parseDouble(str);
                    } catch (NumberFormatException e3) {
                        throw e3;
                    }
                }
            }
        }
        return number;
    }

    public static class MapDeserializer implements JsonDeserializer<Map<String, Object>> {
        @Override
        public Map<String, Object> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            Map<String, Object> m = new LinkedTreeMap<>();
            JsonObject jo = json.getAsJsonObject();
            for (Map.Entry<String, JsonElement> mx : jo.entrySet()) {
                Object value;
                if (mx.getValue().isJsonPrimitive()) {
                    try {
                        value = parse(mx.getValue().getAsString());
                    } catch (Exception e) {
                        value = context.deserialize(mx.getValue(), Object.class);
                    }
                    m.put(mx.getKey(), value);
                } else if (mx.getValue().isJsonArray()) {
                    m.put(mx.getKey(), context.deserialize(mx.getValue(), List.class));
                } else if (mx.getValue().isJsonObject()) {
                    m.put(mx.getKey(), context.deserialize(mx.getValue(), Map.class));
                } else if (mx.getValue().isJsonNull()) {
                    m.put(mx.getKey(), null);
                }
            }
            return m;
        }
    }

    public static class ListDeserializer implements JsonDeserializer<List<Object>> {
        @Override
        public List<Object> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            List<Object> l = new ArrayList<>();
            JsonArray ja = json.getAsJsonArray();
            for (JsonElement ax : ja) {
                Object value;
                if (ax.isJsonPrimitive()) {
                    try {
                        value = parse(ax.getAsString());
                    } catch (Exception e) {
                        value = context.deserialize(ax, Object.class);
                    }
                    l.add(value);
                } else if (ax.isJsonArray()) {
                    l.add(context.deserialize(ax, List.class));
                } else if (ax.isJsonObject()) {
                    l.add(context.deserialize(ax, Map.class));
                } else if (ax.isJsonNull()) {
                    l.add(null);
                }
            }
            return l;
        }
    }
}
