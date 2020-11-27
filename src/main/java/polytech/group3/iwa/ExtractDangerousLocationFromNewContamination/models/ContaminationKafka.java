package polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.models;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;

public class ContaminationKafka {

    private String id_keycloak;

    private int id_case_type;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime reporting_date;

    public ContaminationKafka() { }

    public ContaminationKafka(String id_keycloak, int id_case_type, LocalDateTime reporting_date) {
        this.id_keycloak = id_keycloak;
        this.id_case_type = id_case_type;
        this.reporting_date = reporting_date;
    }

    public String getId_keycloak() {
        return id_keycloak;
    }

    public void setId_keycloak(String id_keycloak) {
        this.id_keycloak = id_keycloak;
    }

    public int getId_case_type() {
        return id_case_type;
    }

    public void setId_case_type(int id_case_type) {
        this.id_case_type = id_case_type;
    }

    public LocalDateTime getReporting_date() {
        return reporting_date;
    }

    public void setReporting_date(LocalDateTime reporting_date) {
        this.reporting_date = reporting_date;
    }

    @Override
    public String toString() {
        return "ContaminationKafka{" +
                "id_keycloak=" + id_keycloak +
                ", id_case_type=" + id_case_type +
                ", reporting_date=" + reporting_date +
                '}';
    }
}
