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
package io.s4.client;

import io.s4.client.util.ObjectBuilder;
import io.s4.collector.EventWrapper;

import java.lang.reflect.Type;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;

public class GenericJsonClientStub extends ClientStub {

    // private static final ObjectBuilder builder = new ObjectBuilder();
    // private static final Gson builder = new Gson();

    private final static InstanceCreator<io.s4.message.Request.Info> infoCreator = new InstanceCreator<io.s4.message.Request.Info>() {
        public io.s4.message.Request.Info createInstance(Type type) {
            return new io.s4.message.Request.ClientInfo();
        }
    };

    private final static Gson builder = (new GsonBuilder()).registerTypeAdapter(io.s4.message.Request.Info.class,
                                                                                infoCreator)
                                                           .create();

    private static final Info protocolInfo = new Info("generic-json", 1, 0);

    @Override
    public Info getProtocolInfo() {
        return protocolInfo;
    }

    @Override
    public EventWrapper eventWrapperFromBytes(byte[] v) {
        try {
            // interpret v as a JSON string
            String s = new String(v);
            JSONObject json = new JSONObject(s);

            String streamName = json.getString("stream");
            String className = json.getString("class");

            Class<?> clazz;
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new ObjectBuilder.Exception("bad class name for json-encoded object: "
                                                          + className,
                                                  e);
            }

            String[] keyNames = null;
            JSONArray keyArray = json.optJSONArray("keys");
            if (keyArray != null) {
                keyNames = new String[keyArray.length()];
                for (int i = 0; i < keyNames.length; ++i) {
                    keyNames[i] = keyArray.optString(i);
                }
            }

            String jevent = json.getString("object");

            Object obj = builder.fromJson(jevent, clazz);

            return new EventWrapper(streamName, keyNames, obj);

        } catch (JSONException e) {
            logger.error("problem with event JSON", e);
        } catch (ObjectBuilder.Exception e) {
            logger.error("failed to build object from JSON", e);
        }

        return null;
    }

    @Override
    public byte[] bytesFromEventWrapper(EventWrapper ew) {
        JSONObject jevent = new JSONObject();

        Object obj = ew.getEvent();

        try {
            jevent.put("stream", ew.getStreamName());
            jevent.put("class", obj.getClass().getName());
            jevent.put("object", builder.toJson(obj));

            return jevent.toString().getBytes();

        } catch (JSONException e) {
            logger.error("exception while converting event wrapper to bytes.",
                         e);
            return null;
        }
    }
}
