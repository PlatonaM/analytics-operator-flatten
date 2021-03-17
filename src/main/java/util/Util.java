package util;/*
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class Util {
    public static String decompress(String data) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(data);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
        StringBuilder resultString = new StringBuilder();
        int b = gzipInputStream.read();
        while (b != -1) {
            resultString.append(((char) b));
            b = gzipInputStream.read();
        }
        gzipInputStream.close();
        return resultString.toString();
    }

    public static String compress(String str) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(str.getBytes());
        gzipOutputStream.close();
        byte[] bytes = outputStream.toByteArray();
        return Base64.getEncoder().withoutPadding().encodeToString(bytes);
    }

    public static String toJSON(List<Map<String, Object>> data) {
        Gson gson = new GsonBuilder().serializeNulls().create();
        Type collectionType = new TypeToken<LinkedList<LinkedTreeMap<String, Object>>>() {
        }.getType();
        return gson.toJson(data, collectionType);
    }
}
