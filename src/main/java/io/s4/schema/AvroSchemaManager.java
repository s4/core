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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

public class AvroSchemaManager extends SchemaManager {
    Map<String, Schema> schemaMap = new ConcurrentHashMap<String, Schema>();

    public void init() {
        super.init();
        for (String schemaName : schemaStringMap.keySet()) {
            String schemaString = schemaStringMap.get(schemaName);
            schemaMap.put(schemaName, Schema.parse(schemaString));
            if (Logger.getLogger("s4").isDebugEnabled()) {
                Logger.getLogger("s4").debug("Putting schema " + schemaName
                        + ": " + schemaMap.get(schemaName));
            }
        }

    }

    public void addSchemaFromFile(String schemaName, String schemaFilename) {
        super.addSchemaFromFile(schemaName, schemaFilename);
        String schemaString = schemaStringMap.get(schemaName);
        schemaMap.put(schemaName, Schema.parse(schemaString));
    }

    public void addSchema(String schemaName, String schemaString) {
        super.addSchema(schemaName, schemaString);
        schemaMap.put(schemaName, Schema.parse(schemaString));
    }

    public Schema getCompiledSchema(String schemaName) {
        return schemaMap.get(schemaName);
    }
}
