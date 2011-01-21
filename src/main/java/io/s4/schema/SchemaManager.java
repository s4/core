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
package io.s4.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class SchemaManager {
    protected Map<String, String> schemaStringMap = new ConcurrentHashMap<String, String>();
    private Map<String, String> schemaFilenameMap;

    public void init() {
        if (schemaFilenameMap == null) {
            return;
        }

        for (String schemaName : schemaFilenameMap.keySet()) {
            String schemaFilename = schemaFilenameMap.get(schemaName);
            addSchemaFromFile(schemaName, schemaFilename);
        }
    }

    public void setSchemas(Map<String, String> schemaFilenameMap) {
        this.schemaFilenameMap = schemaFilenameMap;
    }

    public String getSchemaString(String schemaName) {
        return schemaStringMap.get(schemaName);
    }

    public void addSchemaFromFile(String schemaName, String schemaFilename) {
        BufferedReader br = null;
        FileReader fr = null;
        try {
            File schemaFile = new File(schemaFilename);
            if (schemaFile.exists()) {
                fr = new FileReader(schemaFilename);
                br = new BufferedReader(fr);
                StringBuffer schemaBuffer = new StringBuffer();
                String line = null;
                while ((line = br.readLine()) != null) {
                    schemaBuffer.append(line);
                }
                schemaStringMap.put(schemaName, schemaBuffer.toString());
            } else {
                Logger.getLogger("s4").error("Missing schema file:"
                        + schemaFilename + " for type:" + schemaName);
            }
        } catch (IOException ioe) {
            Logger.getLogger("s4").error("Exception reading schema file "
                                                 + schemaFilename,
                                         ioe);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                }
            }
            if (fr != null) {
                try {
                    fr.close();
                } catch (Exception e) {
                }

            }
        }
    }

    public void addSchema(String schemaName, String schema) {
        schemaStringMap.put(schemaName, schema);
    }
}
