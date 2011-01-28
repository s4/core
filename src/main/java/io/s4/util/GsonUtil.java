package io.s4.util;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class GsonUtil {

    /**
     * This is a workaround for a bug in Gson:
     * 
     * http://code.google.com/p/google-gson/issues/detail?id=279
     * "Templated collections of collections do not serialize correctly"
     */
    private final static JsonSerializer<Object> objectSerializer = new JsonSerializer<Object>() {
        public JsonElement serialize(Object src, Type typeOfSrc,
                                     JsonSerializationContext context) {

            if (src.getClass() != Object.class) {
                return context.serialize(src, src.getClass());
            }

            return new JsonObject();
        }
    };

    private static HashMap<Type, Object> typeAdapters = new HashMap<Type, Object>();

    private volatile static Gson gson = null;

    // build gson ASAP
    static {
        build();
    }

    /**
     * Add a type adapter to the chain.
     * 
     * @param type
     * @param typeAdapter
     */
    public static synchronized void registerTypeAdapter(Type type,
                                                        Object typeAdapter) {
        typeAdapters.put(type, typeAdapter);
        build();
    }

    /**
     * Get a Gson instance.
     * 
     * @return Gson instance with all the adapters registered.
     */
    public static Gson get() {
        return gson;
    }

    private static synchronized void build() {
        GsonBuilder b = (new GsonBuilder()).registerTypeAdapter(Object.class,
                                                                objectSerializer);

        for (Map.Entry<Type, Object> e : typeAdapters.entrySet()) {
            b.registerTypeAdapter(e.getKey(), e.getValue());
        }

        gson = b.create();
    }
}
