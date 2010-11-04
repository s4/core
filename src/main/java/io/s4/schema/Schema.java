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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    private Map<String, Property> properties = new HashMap<String, Property>();
    @SuppressWarnings("unchecked")
    private Class type;

    public Map<String, Property> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @SuppressWarnings("unchecked")
    public Class getType() {
        return type;
    }

    @SuppressWarnings("unchecked")
    public Schema(Class clazz) {
        type = clazz;
        for (Method method : clazz.getMethods()) {
            if (!method.getName().substring(0, 3).equals("set")
                    || !method.getReturnType().equals(Void.TYPE)
                    || method.getParameterTypes().length != 1) {
                continue;
            }

            String getterMethodName = method.getName().replaceFirst("set",
                                                                    "get");

            String propertyName = "";
            if (method.getName().length() > 4) {
                propertyName = method.getName().substring(4);
            }
            propertyName = method.getName().substring(3, 4).toLowerCase()
                    + propertyName;

            Type parameterType = method.getGenericParameterTypes()[0];

            Method getterMethod = null;
            try {
                getterMethod = clazz.getMethod(getterMethodName, new Class[] {});
                /*
                 * if (!getterMethod.getReturnType().equals(parameterType)) {
                 * getterMethod = null; }
                 */
            } catch (NoSuchMethodException nsme) {
                // this is an acceptable possibility, ignore
                // nsme.printStackTrace();
            }

            Property property = Property.getProperty(propertyName,
                                                     getterMethod,
                                                     method,
                                                     parameterType);
            if (property != null) {
                properties.put(propertyName, property);
            }
        }
    }

    public String toString() {
        return this.toString("");
    }

    public String toString(String indent) {
        StringBuffer sb = new StringBuffer();
        sb.append(indent).append("{\n");
        for (Property property : properties.values()) {
            sb.append(property.toString(indent + "   "));
        }

        sb.append(indent).append("}\n");
        return sb.toString();
    }

    static public class Property {
        @SuppressWarnings("unchecked")
        private static List<Class> nonBeanClasses = new ArrayList<Class>();
        static {
            nonBeanClasses.add(String.class);
            nonBeanClasses.add(Number.class);
        }
        @SuppressWarnings("unchecked")
        private Class type;
        private Property componentProperty;
        private Schema schema;
        private String name;
        private Method getterMethod;
        private Method setterMethod;
        private boolean isList;
        private boolean isNumber;

        @SuppressWarnings("unchecked")
        public Class getType() {
            return type;
        }

        public Schema getSchema() {
            return schema;
        }

        public String getName() {
            return name;
        }

        public Method getGetterMethod() {
            return getterMethod;
        }

        public Method getSetterMethod() {
            return setterMethod;
        }

        public boolean isList() {
            return isList;
        }

        public boolean isNumber() {
            return isNumber;
        }

        public Property getComponentProperty() {
            return componentProperty;
        }

        public static Property getProperty(String propertyName, Method getterMethod, Method setterMethod, Type parameterType) {
            if (parameterType instanceof Class) {
                Class ptClass = (Class) parameterType;

                if (ptClass.isArray()) {
                    Class componentType = ptClass.getComponentType();
                    return new Property(propertyName,
                                        getterMethod,
                                        setterMethod,
                                        ptClass,
                                        getProperty("component",
                                                    null,
                                                    null,
                                                    componentType));
                } else if (ptClass.isPrimitive()) {
                    return new Property(propertyName,
                                        getterMethod,
                                        setterMethod,
                                        ptClass);
                }

                List<Class> hierarchy = getHieararchy(ptClass);

                for (Class nonBeanClass : nonBeanClasses) {
                    if (hierarchy.contains(nonBeanClass)) {
                        return new Property(propertyName,
                                            getterMethod,
                                            setterMethod,
                                            ptClass);
                    }
                }

                // if here, then must have no-arg constructor and some bean-like
                // properties
                boolean noArgConstructorFound = false;
                for (Constructor constructor : ptClass.getConstructors()) {
                    if (constructor.getGenericParameterTypes().length == 0) {
                        noArgConstructorFound = true;
                        break;
                    }
                }

                if (!noArgConstructorFound) {
                    return null;
                }

                Schema schema = new Schema(ptClass);
                if (schema.getProperties().size() == 0) {
                    return null;
                }

                return new Property(propertyName,
                                    getterMethod,
                                    setterMethod,
                                    ptClass,
                                    schema);
            } else if (parameterType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) parameterType;
                Class rawType = (Class) pt.getRawType();

                Class[] interfaces = rawType.getInterfaces();
                if (!rawType.equals(List.class)
                        && !Arrays.asList(interfaces).contains(List.class)) {
                    return null;
                }

                Type[] parameterInstantiations = pt.getActualTypeArguments();
                if (parameterInstantiations.length != 1) {
                    return null;
                }

                Type pIType = parameterInstantiations[0];

                return new Property(propertyName,
                                    getterMethod,
                                    setterMethod,
                                    rawType,
                                    getProperty("component", null, null, pIType));
            }

            return null;
        }

        private static List<Class> getHieararchy(Class clazz) {
            List<Class> hierarchy = new ArrayList<Class>();

            for (Class level = clazz; level != null; level = level.getSuperclass()) {
                hierarchy.add(level);
            }
            return hierarchy;
        }

        private Property(String name, Method getterMethod, Method setterMethod,
                Class type) {
            this.name = name;
            this.getterMethod = getterMethod;
            this.setterMethod = setterMethod;
            this.type = type;

            Class[] interfaces = type.getInterfaces();
            if (type.equals(List.class)
                    || Arrays.asList(interfaces).contains(List.class)) {
                isList = true;
            } else if (getHieararchy(type).contains(Number.class)) {
                isNumber = true;
            }
        }

        private Property(String name, Method getterMethod, Method setterMethod,
                Class type, Schema schema) {
            this(name, getterMethod, setterMethod, type);
            this.schema = schema;
        }

        private Property(String name, Method getterMethod, Method setterMethod,
                Class type, Property componentProperty) {
            this(name, getterMethod, setterMethod, type);
            this.componentProperty = componentProperty;
        }

        public String toString() {
            return toString("");
        }

        public String toString(String indent) {
            StringBuffer sb = new StringBuffer();
            sb.append(indent)
              .append(type.getName())
              .append(" ")
              .append(name)
              .append("\n");
            if (schema != null) {
                sb.append(schema.toString(indent));
            }
            if (componentProperty != null) {
                sb.append(componentProperty.toString(indent));
            }

            return sb.toString();
        }
    }
}