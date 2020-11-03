package com.gpsservice.mappers;

import java.util.HashMap;
import java.util.Map;

public class TypeColorMapper {
    final static Map<Integer, String> typeColours = new HashMap<>();

    static {
        typeColours.put(19, "green");
        typeColours.put(20, "blue");
        typeColours.put(21, "yellow");
        typeColours.put(22, "purple");
        typeColours.put(26, "red");
    }

    public String getColorFromType(int type) {
        return typeColours.getOrDefault(type, "orange");
    }
}
