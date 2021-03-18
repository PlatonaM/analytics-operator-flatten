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
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Compression.compress;
import static org.infai.ses.platonam.util.Compression.decompress;
import static org.infai.ses.platonam.util.Json.*;
import static org.infai.ses.platonam.util.Logger.getLogger;


public class Flatten extends BaseOperator {

    private static final Logger logger = getLogger(Flatten.class.getName());
    private final FieldBuilder fieldBuilder;
    private final boolean compressedInput;
    private final boolean compressedOutput;

    public Flatten(FieldBuilder fieldBuilder, boolean compressedInput, boolean compressedOutput) {
        this.fieldBuilder = fieldBuilder;
        this.compressedInput = compressedInput;
        this.compressedOutput = compressedOutput;
    }

    private void outputMessage(Message message, List<Map<String, Object>> data, Map<String, Object> metaData) {
        if (compressedOutput) {
            try {
                message.output("data", compress(toJSON(data)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            message.output("data", toJSON(data));
        }
        message.output("meta_data", toJSON(metaData));
    }

    @Override
    public void run(Message message) {
        List<Map<String, Object>> data;
        Set<String> fields = new HashSet<>();
        try {
            Map<String, Object> metaData = typeSafeMapFromJson(message.getInput("meta_data").getString());
            if (compressedInput) {
                data = typeSafeMapListFromJson(decompress(message.getInput("data").getString()));
            } else {
                data = typeSafeMapListFromJson(message.getInput("data").getString());
            }
            logger.info("received message containing '" + data.size() + "' data points");
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
            metaData.put("sum_fields", fields);
            logger.fine("generated " + fields.size() + " new fields");
            outputMessage(message, data, metaData);
        } catch (Throwable t) {
            logger.severe("error handling message:");
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
