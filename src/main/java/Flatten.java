/*
 * Copyright 2021 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;
import util.Deserializer;
import util.Util;

import java.io.IOException;
import java.util.*;


public class Flatten extends BaseOperator {

    private final FieldBuilder fieldBuilder;
    private final boolean compressedInput;
    private final boolean compressedOutput;

    public Flatten(FieldBuilder fieldBuilder, boolean compressedInput, boolean compressedOutput) {
        this.fieldBuilder = fieldBuilder;
        this.compressedInput = compressedInput;
        this.compressedOutput = compressedOutput;
    }

    private void outputMessage(Message message, List<Map<String, Object>> data, String metaData) {
        if (compressedOutput) {
            try {
                message.output("data", Util.compress(Util.toJSON(data)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            message.output("data", Util.toJSON(data));
        }
        message.output("meta_data", metaData);
    }

    @Override
    public void run(Message message) {
        List<Map<String, Object>> data;
        Set<String> fields = new HashSet<>();
        try {
            String metaData = message.getInput("meta_data").getString();
            GsonBuilder builder = new GsonBuilder();
            builder.registerTypeAdapter(Map.class, new Deserializer.MapDeserializer());
            builder.registerTypeAdapter(List.class, new Deserializer.ListDeserializer());
            Gson gson = builder.create();
            if (compressedInput) {
                data = gson.fromJson(Util.decompress(message.getInput("data").getString()), new TypeToken<List<Map<String, Object>>>() {
                }.getType());
            } else {
                data = gson.fromJson(message.getInput("data").getString(), new TypeToken<List<Map<String, Object>>>() {
                }.getType());
            }
            System.out.println("received message with '" + data.size() + "' data points");
            for (Map<String, Object> msg : data) {
                for (String rootField : fieldBuilder.rootFields()) {
                    if (msg.get(rootField) != null) {
                        for (Object item : (ArrayList<?>) msg.get(rootField)) {
                            LinkedTreeMap<?, ?> itemData = (LinkedTreeMap<?, ?>) item;
                            String field = fieldBuilder.buildField(rootField, itemData);
                            fields.add(field);
                            msg.put(field, (Integer) msg.getOrDefault(field, 0) + 1);
                        }
                    }
                    msg.remove(rootField);
                }
            }
            for (Map<String, Object> msg : data) {
                for (String field : fields) {
                    msg.putIfAbsent(field, 0);
                }
            }
            outputMessage(message, data, metaData);
        } catch (Throwable t) {
            System.out.println("error handling message:");
            t.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addInput("data");
        message.addInput("meta_data");
        return message;
    }

}
