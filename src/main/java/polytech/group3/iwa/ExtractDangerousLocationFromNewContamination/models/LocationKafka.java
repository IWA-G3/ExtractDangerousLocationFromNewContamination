package polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.models;


public class LocationKafka {

    private double latitude;

    private double longitude;

    private String location_date;

    private String userid;

    public LocationKafka() {
        super();
    }

    public LocationKafka(double latitude, double longitude, String location_date, String userid) {
        super();
        this.latitude = latitude;
        this.longitude = longitude;
        this.location_date = location_date;
        this.userid = userid;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getLocation_date() {
        return location_date;
    }

    public void setLocation_date(String location_date) {
        this.location_date = location_date;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    @Override
    public String toString() {
        return "LocationKafka{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                ", location_date=" + location_date +
                ", userid=" + userid +
                '}';
    }
}
