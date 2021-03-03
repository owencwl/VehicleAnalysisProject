package com.umxwe.fakeplatevehicle;

/**
 * @ClassName UmxDistanceUnit
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/3/3
 */

import java.io.IOException;

/**
 * The DistanceUnit enumerates several units for measuring distances. These units
 * provide methods for converting strings and methods to convert units among each
 * others. Some methods like {@link DistanceUnit#getEarthCircumference} refer to
 * the earth ellipsoid defined in {@link GeoUtils}. The default unit used within
 * this project is <code>METERS</code> which is defined by <code>DEFAULT</code>
 */
enum DistanceUnit  {
    INCH(0.0254, "in", "inch"),
    YARD(0.9144, "yd", "yards"),
    FEET(0.3048, "ft", "feet"),
    KILOMETERS(1000.0, "km", "kilometers"),
    NAUTICALMILES(1852.0, "NM", "nmi", "nauticalmiles"),
    MILLIMETERS(0.001, "mm", "millimeters"),
    CENTIMETERS(0.01, "cm", "centimeters"),

    // 'm' is a suffix of 'nmi' so it must follow 'nmi'
    MILES(1609.344, "mi", "miles"),

    // since 'm' is suffix of other unit
    // it must be the last entry of unit
    // names ending with 'm'. otherwise
    // parsing would fail
    METERS(1, "m", "meters");

    public static final DistanceUnit DEFAULT = METERS;

    private double meters;
    private final String[] names;

    DistanceUnit(double meters, String...names) {
        this.meters = meters;
        this.names = names;
    }

    /**
     * Convert a value into meters
     *
     * @param distance distance in this unit
     * @return value in meters
     */
    public double toMeters(double distance) {
        return convert(distance, this, DistanceUnit.METERS);
    }

    /**
     * Convert a value given in meters to a value of this unit
     *
     * @param distance distance in meters
     * @return value in this unit
     */
    public double fromMeters(double distance) {
        return convert(distance, DistanceUnit.METERS, this);
    }

    /**
     * Convert a given value into another unit
     *
     * @param distance value in this unit
     * @param unit source unit
     * @return value in this unit
     */
    public double convert(double distance, DistanceUnit unit) {
        return convert(distance, unit, this);
    }

    /**
     * Convert a value to a distance string
     *
     * @param distance value to convert
     * @return String representation of the distance
     */
    public String toString(double distance) {
        return distance + toString();
    }

    @Override
    public String toString() {
        return names[0];
    }

    /**
     * Converts the given distance from the given DistanceUnit, to the given DistanceUnit
     *
     * @param distance Distance to convert
     * @param from     Unit to convert the distance from
     * @param to       Unit of distance to convert to
     * @return Given distance converted to the distance in the given unit
     */
    public static double convert(double distance, DistanceUnit from, DistanceUnit to) {
        if (from == to) {
            return distance;
        } else {
            return distance * from.meters / to.meters;
        }
    }

    /**
     * Parses a given distance and converts it to the specified unit.
     *
     * @param distance String defining a distance (value and unit)
     * @param defaultUnit unit assumed if none is defined
     * @param to unit of result
     * @return parsed distance
     */
    public static double parse(String distance, DistanceUnit defaultUnit, DistanceUnit to) {
        Distance dist = Distance.parseDistance(distance, defaultUnit);
        return convert(dist.value, dist.unit, to);
    }

    /**
     * Parses a given distance and converts it to this unit.
     *
     * @param distance String defining a distance (value and unit)
     * @param defaultUnit unit to expect if none if provided
     * @return parsed distance
     */
    public double parse(String distance, DistanceUnit defaultUnit) {
        return parse(distance, defaultUnit, this);
    }

    /**
     * Convert a String to a {@link DistanceUnit}
     *
     * @param unit name of the unit
     * @return unit matching the given name
     * @throws IllegalArgumentException if no unit matches the given name
     */
    public static DistanceUnit fromString(String unit) {
        for (DistanceUnit dunit : values()) {
            for (String name : dunit.names) {
                if(name.equals(unit)) {
                    return dunit;
                }
            }
        }
        throw new IllegalArgumentException("No distance unit match [" + unit + "]");
    }

    /**
     * Parses the suffix of a given distance string and return the corresponding {@link DistanceUnit}
     *
     * @param distance string representing a distance
     * @param defaultUnit default unit to use, if no unit is provided by the string
     * @return unit of the given distance
     */
    public static DistanceUnit parseUnit(String distance, DistanceUnit defaultUnit) {
        for (DistanceUnit unit : values()) {
            for (String name : unit.names) {
                if(distance.endsWith(name)) {
                    return unit;
                }
            }
        }
        return defaultUnit;
    }

    /**
     * This class implements a value+unit tuple.
     */
    public static class Distance implements Comparable<Distance> {
        public final double value;
        public final DistanceUnit unit;

        public Distance(double value, DistanceUnit unit) {
            super();
            this.value = value;
            this.unit = unit;
        }

        /**
         * Converts a {@link Distance} value given in a specific {@link DistanceUnit} into
         * a value equal to the specified value but in a other {@link DistanceUnit}.
         *
         * @param unit unit of the result
         * @return converted distance
         */
        public Distance convert(DistanceUnit unit) {
            if(this.unit == unit) {
                return this;
            } else {
                return new Distance(DistanceUnit.convert(value, this.unit, unit), unit);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null) {
                return false;
            } else if (obj instanceof Distance) {
                Distance other = (Distance) obj;
                return DistanceUnit.convert(value, unit, other.unit) == other.value;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Double.valueOf(value * unit.meters).hashCode();
        }

        @Override
        public int compareTo(Distance o) {
            return Double.compare(value, DistanceUnit.convert(o.value, o.unit, unit));
        }

        @Override
        public String toString() {
            return unit.toString(value);
        }

        /**
         * Parse a {@link Distance} from a given String. If no unit is given
         * <code>DistanceUnit.DEFAULT</code> will be used
         *
         * @param distance String defining a {@link Distance}
         * @return parsed {@link Distance}
         */
        public static Distance parseDistance(String distance) {
            return parseDistance(distance, DEFAULT);
        }

        /**
         * Parse a {@link Distance} from a given String
         *
         * @param distance String defining a {@link Distance}
         * @param defaultUnit {@link DistanceUnit} to be assumed
         *          if not unit is provided in the first argument
         * @return parsed {@link Distance}
         */
        private static Distance parseDistance(String distance, DistanceUnit defaultUnit) {
            for (DistanceUnit unit : values()) {
                for (String name : unit.names) {
                    if(distance.endsWith(name)) {
                        return new Distance(Double.parseDouble(distance.substring(0, distance.length() - name.length())), unit);
                    }
                }
            }
            return new Distance(Double.parseDouble(distance), defaultUnit);
        }
    }
}