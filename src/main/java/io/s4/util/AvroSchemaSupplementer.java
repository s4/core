/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;

import org.json.JSONArray;
import org.json.JSONObject;

public class AvroSchemaSupplementer {

    public static void main(String args[]) {
        if (args.length < 1) {
            System.err.println("No schema filename specified");
            System.exit(1);
        }

        String filename = args[0];
        FileReader fr = null;
        BufferedReader br = null;
        InputStreamReader isr = null;
        try {
            if (filename == "-") {
                isr = new InputStreamReader(System.in);
                br = new BufferedReader(isr);
            } else {
                fr = new FileReader(filename);
                br = new BufferedReader(fr);
            }

            String inputLine = "";
            StringBuffer jsonBuffer = new StringBuffer();
            while ((inputLine = br.readLine()) != null) {
                jsonBuffer.append(inputLine);
            }

            JSONObject jsonRecord = new JSONObject(jsonBuffer.toString());

            JSONObject keyPathElementSchema = new JSONObject();
            keyPathElementSchema.put("name", "KeyPathElement");
            keyPathElementSchema.put("type", "record");

            JSONArray fieldsArray = new JSONArray();
            JSONObject fieldRecord = new JSONObject();
            fieldRecord.put("name", "index");
            JSONArray typeArray = new JSONArray("[\"int\", \"null\"]");
            fieldRecord.put("type", typeArray);
            fieldsArray.put(fieldRecord);
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "keyName");
            typeArray = new JSONArray("[\"string\", \"null\"]");
            fieldRecord.put("type", typeArray);
            fieldsArray.put(fieldRecord);

            keyPathElementSchema.put("fields", fieldsArray);

            JSONObject keyInfoSchema = new JSONObject();
            keyInfoSchema.put("name", "KeyInfo");
            keyInfoSchema.put("type", "record");

            fieldsArray = new JSONArray();
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "keyPath");
            typeArray = new JSONArray("[\"string\", \"null\"]");
            fieldRecord.put("type", typeArray);
            fieldsArray.put(fieldRecord);
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "fullKeyPath");
            typeArray = new JSONArray("[\"string\", \"null\"]");
            fieldRecord.put("type", typeArray);
            fieldsArray.put(fieldRecord);
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "keyPathElementList");
            JSONObject typeRecord = new JSONObject();
            typeRecord.put("type", "array");
            typeRecord.put("items", keyPathElementSchema);
            fieldRecord.put("type", typeRecord);
            fieldsArray.put(fieldRecord);

            keyInfoSchema.put("fields", fieldsArray);

            JSONObject partitionInfoSchema = new JSONObject();
            partitionInfoSchema.put("name", "PartitionInfo");
            partitionInfoSchema.put("type", "record");
            fieldsArray = new JSONArray();
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "partitionId");
            typeArray = new JSONArray("[\"int\", \"null\"]");
            fieldRecord.put("type", typeArray);
            fieldsArray.put(fieldRecord);
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "compoundKey");
            typeArray = new JSONArray("[\"string\", \"null\"]");
            fieldRecord.put("type", typeArray);
            fieldsArray.put(fieldRecord);
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "compoundValue");
            typeArray = new JSONArray("[\"string\", \"null\"]");
            fieldRecord.put("type", typeArray);
            fieldsArray.put(fieldRecord);
            fieldRecord = new JSONObject();
            fieldRecord.put("name", "keyInfoList");
            typeRecord = new JSONObject();
            typeRecord.put("type", "array");
            typeRecord.put("items", keyInfoSchema);
            fieldRecord.put("type", typeRecord);
            fieldsArray.put(fieldRecord);

            partitionInfoSchema.put("fields", fieldsArray);

            fieldRecord = new JSONObject();
            fieldRecord.put("name", "S4__PartitionInfo");
            typeRecord = new JSONObject();
            typeRecord.put("type", "array");
            typeRecord.put("items", partitionInfoSchema);
            fieldRecord.put("type", typeRecord);

            fieldsArray = jsonRecord.getJSONArray("fields");
            fieldsArray.put(fieldRecord);

            System.out.println(jsonRecord.toString(3));
        } catch (Exception ioe) {
            throw new RuntimeException(ioe);
        } finally {
            if (br != null)
                try {
                    br.close();
                } catch (Exception e) {
                }
            if (isr != null)
                try {
                    isr.close();
                } catch (Exception e) {
                }
            if (fr != null)
                try {
                    fr.close();
                } catch (Exception e) {
                }
        }
    }

}
