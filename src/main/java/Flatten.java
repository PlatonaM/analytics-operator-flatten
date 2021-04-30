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
import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.util.*;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Logger.getLogger;


public class Flatten extends BaseOperator {

    private static final Logger logger = getLogger(Flatten.class.getName());
    private final FieldBuilder fieldBuilder;
    private final Map<String, String> inputMap;
    private final String targetInput;

    public Flatten(FieldBuilder fieldBuilder, Map<String, String> inputMap, String targetInput) {
        if (targetInput == null || targetInput.isBlank()) {
            throw new RuntimeException("invalid target_input");
        }
        this.fieldBuilder = fieldBuilder;
        this.inputMap = inputMap;
        this.targetInput = targetInput;
    }

    private void outputMessage(Message message, Map<String, Object> flatData, Map<String, Object> defaultValues, Map<String, Object> relayData) {
        message.output("flat_data", Json.toString(new TypeToken<Map<String, Object>>() {
        }.getType(), flatData));
        message.output("default_values", Json.toString(new TypeToken<Map<String, Object>>() {
        }.getType(), defaultValues));
        for (Map.Entry<String, Object> entry : relayData.entrySet()) {
            message.output(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void run(Message message) {
        Map<String, Object> targetData = null;
        Map<String, Object> relayData = new HashMap<>();
        Set<String> fields = new HashSet<>();
        try {
            for (Map.Entry<String, String> entry : inputMap.entrySet()) {
                try {
                    if (entry.getKey().equals(targetInput)) {
                        targetData = Json.fromString(message.getInput(entry.getKey()).getString(), new TypeToken<>() {
                        });
                    } else {
                        relayData.put(entry.getKey(), message.getInput(entry.getKey()).getValue(Object.class));
                    }
                } catch (NoValueException e) {
                    if (entry.getKey().equals(targetInput)) {
                        targetData = new HashMap<>();
                    } else {
                        relayData.put(entry.getKey(), null);
                    }
                }
            }
            for (String rootField : fieldBuilder.rootFields()) {
                if (targetData.get(rootField) != null) {
                    for (Object item : (ArrayList<?>) targetData.get(rootField)) {
                        LinkedTreeMap<?, ?> itemData = (LinkedTreeMap<?, ?>) item;
                        String field = fieldBuilder.buildField(rootField, itemData);
                        fields.add(field);
                        targetData.put(field, (Integer) targetData.getOrDefault(field, 0) + 1);
                    }
                }
                targetData.remove(rootField);
            }
            Map<String, Object> defaultValues = new HashMap<>();
            for (String field : fields) {
                defaultValues.put(field, 0);
            }
            outputMessage(message, targetData, defaultValues, relayData);
        } catch (Throwable t) {
            logger.severe("error handling message:");
            t.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        for (String key : inputMap.keySet()) {
            message.addInput(key);
        }
        return message;
    }

}
