package com.ecommerce.analytics.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EnrichedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    // Original event fields (snake_case to match JSON)
    private String event_id;
    private String event_type;
    private String user_id;
    private String timestamp;
    private Map<String, Object> metadata = new HashMap<>();
    private Map<String, Object> simulation = new HashMap<>();

    // Enriched fields (also snake_case for consistency)
    private long processing_time;
    private String user_segment;
    private double customer_ltv;
    private double risk_score;

    // Default constructor for serialization
    public EnrichedEvent() {
    }

    // Constructor from CustomerEvent
    public EnrichedEvent(CustomerEvent event) {
        this.event_id = event.getEventId();
        this.event_type = event.getEventType();
        this.user_id = event.getUserId();
        this.timestamp = event.getTimestamp();
        this.metadata = event.getMetadata();
        this.simulation = event.getSimulation();

        // Set default values for enriched fields
        this.processing_time = System.currentTimeMillis();
        this.user_segment = "unknown";
        this.customer_ltv = 0.0;
        this.risk_score = 0.0;
    }

    // Getters and setters
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

    public long getProcessingTime() {
        return processing_time;
    }

    public void setProcessingTime(long processingTime) {
        this.processing_time = processingTime;
    }

    public String getUserSegment() {
        return user_segment;
    }

    public void setUserSegment(String userSegment) {
        this.user_segment = userSegment;
    }

    public double getCustomerLtv() {
        return customer_ltv;
    }

    public void setCustomerLtv(double customerLtv) {
        this.customer_ltv = customerLtv;
    }

    public double getRiskScore() {
        return risk_score;
    }

    public void setRiskScore(double riskScore) {
        this.risk_score = riskScore;
    }

    // Direct field accessors for JSON serialization/deserialization
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

    public long getProcessing_time() {
        return processing_time;
    }

    public void setProcessing_time(long processing_time) {
        this.processing_time = processing_time;
    }

    public String getUser_segment() {
        return user_segment;
    }

    public void setUser_segment(String user_segment) {
        this.user_segment = user_segment;
    }

    public double getCustomer_ltv() {
        return customer_ltv;
    }

    public void setCustomer_ltv(double customer_ltv) {
        this.customer_ltv = customer_ltv;
    }

    public double getRisk_score() {
        return risk_score;
    }

    public void setRisk_score(double risk_score) {
        this.risk_score = risk_score;
    }
}