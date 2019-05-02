package com.rogerguo.data;

import com.rogerguo.common.DateUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class TaxiData {

    private String medallion;

    private String hackLicense;

    private String vendorId;

    private String rateCode;

    private String storeAndFwdFlag;

    private int passengerCount;

    private double tripTimeInSecs;

    private double tripDistance;

    private double longitude;

    private double latitude;

    private Date date;

    public List<TaxiData> parseData(String inputFile) {
        List<TaxiData> taxiDataList = new ArrayList<>();

        URL input = getClass().getClassLoader().getResource(inputFile);
        if (input == null) {
            throw new RuntimeException("Couldn't load resource trip_data_1_pickup.csv");
        }

        // date parser corresponding to the CSV format
        //DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);


        try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            for (CSVRecord record : parser) {
                TaxiData taxiData = new TaxiData();
                // pull out the fields corresponding to our simple feature attributes
                taxiData.setMedallion(record.get(0));
                taxiData.setHackLicense(record.get(1));
                taxiData.setVendorId(record.get(2));
                taxiData.setRateCode(record.get(3));
                taxiData.setStoreAndFwdFlag(record.get(4));
                taxiData.setPassengerCount(Integer.parseInt(record.get(6)));
                taxiData.setTripTimeInSecs(Double.parseDouble(record.get(7)));
                taxiData.setTripDistance(Double.parseDouble(record.get(8)));

                //taxiData.setDate(Date.from(LocalDateTime.parse(record.get(5), dateFormat).toInstant(ZoneOffset.UTC)));
                taxiData.setDate(DateUtil.parseDateString(record.get(5)));

                double longitude = Double.parseDouble(record.get(9));
                double latitude = Double.parseDouble(record.get(10));
                taxiData.setLongitude(longitude);
                taxiData.setLatitude(latitude);

                taxiDataList.add(taxiData);

            }

        } catch (IOException e) {
            throw new RuntimeException("Error reading taxi data:", e);
        }

        return taxiDataList;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }


    public String getMedallion() {
        return medallion;
    }

    public void setMedallion(String medallion) {
        this.medallion = medallion;
    }

    public String getHackLicense() {
        return hackLicense;
    }

    public void setHackLicense(String hackLicense) {
        this.hackLicense = hackLicense;
    }

    public String getVendorId() {
        return vendorId;
    }

    public void setVendorId(String vendorId) {
        this.vendorId = vendorId;
    }

    public String getRateCode() {
        return rateCode;
    }

    public void setRateCode(String rateCode) {
        this.rateCode = rateCode;
    }

    public String getStoreAndFwdFlag() {
        return storeAndFwdFlag;
    }

    public void setStoreAndFwdFlag(String storeAndFwdFlag) {
        this.storeAndFwdFlag = storeAndFwdFlag;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public void setPassengerCount(int passengerCount) {
        this.passengerCount = passengerCount;
    }

    public double getTripTimeInSecs() {
        return tripTimeInSecs;
    }

    public void setTripTimeInSecs(double tripTimeInSecs) {
        this.tripTimeInSecs = tripTimeInSecs;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public void setTripDistance(double tripDistance) {
        this.tripDistance = tripDistance;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
