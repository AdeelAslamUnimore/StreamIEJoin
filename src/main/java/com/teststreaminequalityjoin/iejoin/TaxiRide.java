package com.teststreaminequalityjoin.iejoin;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TaxiRide implements Serializable {
    private static DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public Date pickupTS, dropoffTS;
    public String pickupCell, dropoffCell, taxiID, license;
    public double fare, tip;
    public static TaxiRide parse(String line) throws AreaMapper.OutOfGridException, ParseException {
        TaxiRide tr = new TaxiRide();

        String[] tokens = line.split(",");

        double puLat = Double.valueOf(tokens[7]);
        double puLong = Double.valueOf(tokens[6]);
        double doLat = Double.valueOf(tokens[9]);
        double doLong = Double.valueOf(tokens[8]);
        tr.pickupCell = AreaMapper.getCellID(puLat, puLong);
        tr.dropoffCell = AreaMapper.getCellID(doLat, doLong);

        tr.taxiID = tokens[0];
        tr.license = tokens[1];
        tr.pickupTS = dateFormat.parse(tokens[2]);
        tr.dropoffTS = dateFormat.parse(tokens[3]);
        tr.fare = Double.valueOf(tokens[11]);
        tr.tip = Double.valueOf(tokens[14]);

        return tr;

    }
}
