package com.manny;

import java.io.Serializable;


public class WeatherMeasurement implements Serializable
{
    //dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
    String dt = null;
    double averageTemperature = 0;
    String averageTemperatureUncertainty = null;
    String city = null;
    String country = null;
    String latitude = null;
    String longitude = null;

    public double getAverageTemperature ()
    {
        return averageTemperature;
    }

    public void setAverageTemperature (double averageTemperature)
    {
        this.averageTemperature = averageTemperature;
    }

    public String getAverageTemperatureUncertainty ()
    {
        return averageTemperatureUncertainty;
    }

    public void setAverageTemperatureUncertainty (String averageTemperatureUncertainty)
    {
        this.averageTemperatureUncertainty = averageTemperatureUncertainty;
    }

    public String getCity ()
    {
        return city;
    }

    public void setCity (String city)
    {
        this.city = city;
    }

    public String getCountry ()
    {
        return country;
    }

    public void setCountry (String country)
    {
        this.country = country;
    }

    public String getDt ()
    {
        return dt;
    }

    public void setDt (String dt)
    {
        this.dt = dt;
    }

    public String getLatitude ()
    {
        return latitude;
    }

    public void setLatitude (String latitude)
    {
        this.latitude = latitude;
    }

    public String getLongitude ()
    {
        return longitude;
    }

    public void setLongitude (String longitude)
    {
        this.longitude = longitude;
    }
}
