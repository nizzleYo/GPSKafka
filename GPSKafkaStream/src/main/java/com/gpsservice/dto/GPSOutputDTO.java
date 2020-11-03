package com.gpsservice.dto;

import java.util.Arrays;

public class GPSOutputDTO {
    public int unit;
    public int type;
    public double[] coordinates;
    public String color;

    public GPSOutputDTO(int unit, int type, double[] coordinates, String color) {
        this.unit = unit;
        this.type = type;
        this.coordinates = coordinates;
        this.color = color;
    }

    @Override
    public String toString() {
        return "{\"unit\":\"" + unit + "\""
                + ", \"type\":\"" + type + "\""
                + ", \"coordinates\":" + Arrays.toString(coordinates)
                + ", \"color\":\"" + color + "\""
                + "}";
    }
}
