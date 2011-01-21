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
package io.s4.client.util;

import io.s4.message.Request;
import io.s4.message.SinglePERequest;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;

import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;

public class ObjectBuilder {

    private ConcurrentHashMap<Class<?>, JSONDeserializer<Object>> deserializers = new ConcurrentHashMap<Class<?>, JSONDeserializer<Object>>();

    private JSONSerializer serializer = (new JSONSerializer()).exclude("class");

    // private JSONSerializer serializer = (new JSONSerializer());

    public Object fromJson(String jevent, Class<?> clazz)
            throws ObjectBuilder.Exception {

        JSONDeserializer<Object> deser = deserializers.get(clazz);

        if (deser == null) {
            JSONDeserializer<Object> newDeser = new JSONDeserializer<Object>();
            newDeser.use(null, clazz);

            deser = deserializers.putIfAbsent(clazz, newDeser);

            if (deser == null)
                deser = newDeser;
        }

        return deser.deserialize(jevent);

    }

    public String toJson(Object e) {
        return serializer.serialize(e);
    }

    public static class Exception extends java.lang.Exception {
        public Exception(String message) {
            super(message);
        }

        public Exception(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class TEST {
        private int a;
        private int b;

        public void setA(int a) {
            this.a = a * 10;
        }

        public String toString() {
            return "" + a + " " + b;
        }

        public int getA() {
            return a;
        }

        public int getB() {
            return b;
        }

        public TEST(int a, int b) {
            this.a = a * 10;
            this.b = b * 10;
        }

        public TEST() {
        }
    }

    public static void main(String[] argv) throws Exception {

        ObjectBuilder b = new ObjectBuilder();

        String s = "{a:5, b:100}";
        Object out = b.fromJson(s, TEST.class);

        System.out.println(out.toString());

        TEST t = new TEST(1, 2);

        System.out.println(b.toJson(t));

        String[] query = { "name", "count", "freq" };
        String target[] = { "ACDW", "11" };

        io.s4.message.Request.ClientInfo rinfo = new io.s4.message.Request.ClientInfo();
        rinfo.setRequesterUUID(UUID.randomUUID());
        Request req = new io.s4.message.SinglePERequest(Arrays.asList(target),
                                                        Arrays.asList(query),
                                                        rinfo);

        System.out.println(req.toString());

        InstanceCreator<io.s4.message.Request.Info> infoCreator = new InstanceCreator<io.s4.message.Request.Info>() {
            public io.s4.message.Request.Info createInstance(Type type) {
                return new io.s4.message.Request.ClientInfo();
            }
        };

        Gson gson = (new GsonBuilder()).registerTypeAdapter(io.s4.message.Request.Info.class,
                                                            infoCreator)
                                       .create();

        System.out.println("gson: " + gson.toJson(req));
        System.out.println("gson reversed: "
                + gson.fromJson(gson.toJson(req), SinglePERequest.class));

        System.out.println(b.toJson(req));
        System.out.println(b.toJson(Arrays.asList(query)));
    }
}
