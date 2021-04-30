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


import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import org.infai.ses.platonam.util.Json;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Logger.getLogger;


public class Flatten extends BaseOperator {

    private static final Logger logger = getLogger(Flatten.class.getName());
    private final FieldBuilder fieldBuilder;

    public Flatten(FieldBuilder fieldBuilder) {
        this.fieldBuilder = fieldBuilder;
    }

    private void outputMessage(Message message, Map<String, Object> flatData, Map<String, Object> defaultValues) throws IOException {
        message.output("flat_data", Json.toString(new TypeToken<Map<String, Object>>() {
        }.getType(), flatData));
        message.output("default_values", Json.toString(new TypeToken<Map<String, Object>>() {
        }.getType(), defaultValues));
    }

    @Override
    public void run(Message message) {
        Map<String, Object> data;
        Set<String> fields = new HashSet<>();
        try {
            data = Json.fromString(message.getInput("data").getString(), new TypeToken<>() {
            });
            for (String rootField : fieldBuilder.rootFields()) {
                if (data.get(rootField) != null) {
                    for (Object item : (ArrayList<?>) data.get(rootField)) {
                        LinkedTreeMap<?, ?> itemData = (LinkedTreeMap<?, ?>) item;
                        String field = fieldBuilder.buildField(rootField, itemData);
                        fields.add(field);
                        data.put(field, (Integer) data.getOrDefault(field, 0) + 1);
                    }
                }
                data.remove(rootField);
            }
            Map<String, Object> defaultValues = new HashMap<>();
            for (String field : fields) {
                defaultValues.put(field, 0);
            }
            outputMessage(message, data, defaultValues);
        } catch (Throwable t) {
            logger.severe("error handling message:");
            t.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addInput("data");
        return message;
    }

}
