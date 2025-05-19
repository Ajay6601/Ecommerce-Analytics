package com.ecommerce.analytics.models;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String event_id;  // Using snake_case to match JSON
    private String event_type;
    private String user_id;
    private String timestamp;
    private Map<String, Object> metadata;
    private Map<String, Object> simulation;  // Added to match input JSON

    public CustomerEvent() {
        this.metadata = new HashMap<>();
        this.simulation = new HashMap<>();
    }

    public String getEventId() {
        return event_id;
    }

    public void setEventId(String eventId) {
        this.event_id = eventId;
    }

    public String getEventType() {
        return event_type;
    }

    public void setEventType(String eventType) {
        this.event_type = eventType;
    }

    public String getUserId() {
        return user_id;
    }

    public void setUserId(String userId) {
        this.user_id = userId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata != null ? metadata : new HashMap<>();
    }

    public Map<String, Object> getSimulation() {
        return simulation;
    }

    public void setSimulation(Map<String, Object> simulation) {
        this.simulation = simulation != null ? simulation : new HashMap<>();
    }

    // For direct JSON field access
    public String getEvent_id() {
        return event_id;
    }

    public void setEvent_id(String event_id) {
        this.event_id = event_id;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }
}