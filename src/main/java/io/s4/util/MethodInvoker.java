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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class to invoke a method on an arbitrary object, if such a method is
 * defined.
 */
public class MethodInvoker {

    /**
     * Find and invoke a getter on an object. A getter for parameter N is a
     * public method whose name equals "get" + N, ignoring case, and which takes
     * zero arguments. If no such method is found, an exception is thrown.
     * 
     * @param obj
     *            object on which getter id to be invoked
     * @param name
     *            parameter name
     * @return value returned by the getter method, if such a method is found.
     *         Null if the object is null.
     * @throws Exception
     *             if no suitable getter is found, or if the getter method
     *             throws an exception. The latter is wrapped in an
     *             {@link InvocationTargetException}
     */
    public static Object invokeGetter(Object obj, String name) throws Exception {
        if (obj != null) {
            Method getter = findGetter(obj.getClass(), name);

            if (getter != null) {
                return getter.invoke(obj);

            } else {
                throw new NoGetterException(obj.getClass(), name);
            }

        } else {
            throw new Exception("Null Target");
        }
    }

    private static ConcurrentHashMap<Class<?>, HashMap<String, Method>> gettersMap = new ConcurrentHashMap<Class<?>, HashMap<String, Method>>();

    private static Method findGetter(Class<?> clazz, String name) {
        HashMap<String, Method> getters = gettersMap.get(clazz);

        if (getters == null) {
            HashMap<String, Method> newGetters = reflectGetters(clazz);

            getters = gettersMap.putIfAbsent(clazz, newGetters);

            if (getters == null)
                getters = newGetters;
        }

        return getters.get(name.toLowerCase());
    }

    private static HashMap<String, Method> reflectGetters(Class<?> clazz) {
        HashMap<String, Method> map = new HashMap<String, Method>();

        for (Method m : clazz.getMethods()) {
            // the method we are interested in should be named get* and take no
            // arguments.
            String n = m.getName();

            if (m.getParameterTypes().length == 0 && n.startsWith("get")) {
                String name = n.substring(3).toLowerCase();

                map.put(name, m);
            }
        }

        return map;
    }

    private static Class<?>[] getTypes(Object[] args) {
        Class<?>[] aT = new Class<?>[args.length];
        for (int i = 0; i < args.length; ++i) {
            aT[i] = args[i].getClass();
        }

        return aT;
    }

    public static class NoGetterException extends Exception {
        public NoGetterException(Class<?> clazz, String name) {
            super("No Getter for attribute " + clazz.getName() + "." + name);
        }
    }

    public static class NoMethodException extends Exception {
        public NoMethodException(Class<?> clazz, String name, Object[] args) {
            super("No method found " + clazz.getName() + "." + name + "("
                    + MethodInvoker.getTypes(args) + ")");
        }
    }

    public static class NullTargetException extends Exception {
        public NullTargetException() {
            super();
        }
    }

}
