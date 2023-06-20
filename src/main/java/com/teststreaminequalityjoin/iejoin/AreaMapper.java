package com.teststreaminequalityjoin.iejoin;

public class AreaMapper {
    public static final double EARTH_RADIUS = 6371; // km
    public static final double GRID_WIDTH = 150; // km
    public static final double CELL_WIDTH = 0.25; // km
    public static final int MAX_CELL = 600;
    public static final int MIN_CELL = 1;

    // The coordinate 41.474937, -74.913585 marks the center of the first cell
    public static final double O_LAT = moveNorth(41.474937, CELL_WIDTH / 2);
    public static final double O_LONG = moveWest(-74.913585, CELL_WIDTH / 2);
    public static final double NE_LAT = O_LAT;
    public static final double NE_LONG = moveEast(O_LONG, GRID_WIDTH);
    public static final double S_LAT = moveSouth(O_LAT, GRID_WIDTH);
    public static final double S_LONG = O_LONG;
    public static final double SE_LAT = S_LAT;
    public static final double SE_LONG = NE_LONG;

    public static String getCellID(double latitude, double longitude) throws OutOfGridException {
        if (!checkCoord(latitude, longitude)) {
            throw new OutOfGridException(latitude, longitude);
        }

        int we = (int) Math.ceil(toKm(O_LONG, longitude) / CELL_WIDTH);
        int ns = (int) Math.ceil(toKm(O_LAT, latitude) / CELL_WIDTH);

        if (we > MAX_CELL ||
                we < MIN_CELL ||
                ns > MAX_CELL ||
                ns < MIN_CELL) {
            throw new OutOfGridException(latitude, longitude);
        }

        return we + "." + ns;
    }

    private static boolean checkCoord(double latitude, double longitude) {
        return !(latitude > O_LAT ||
                latitude < S_LAT ||
                longitude > NE_LONG ||
                longitude < O_LONG);
    }

    private static double toDeg(double km) {
        return Math.toDegrees(km / EARTH_RADIUS);
    }

    private static double toKm(double startDeg, double endDeg) {
        return Math.abs(Math.toRadians(endDeg - startDeg) * EARTH_RADIUS);
    }

    private static double moveNorth(double latitude, double km) {
        return latitude + toDeg(km);
    }

    private static double moveSouth(double latitude, double km) {
        return latitude - toDeg(km);
    }

    private static double moveEast(double longitude, double km) {
        return longitude + toDeg(km);
    }

    private static double moveWest(double longitude, double km) {
        return longitude - toDeg(km);
    }

    public static class OutOfGridException extends Exception {
        private double latitude;
        private double longitude;

        public OutOfGridException(double latitude, double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        @Override
        public String getMessage() {
            return String.valueOf(latitude) + ", " + String.valueOf(longitude) + ": out of bounds";
        }
    }
}