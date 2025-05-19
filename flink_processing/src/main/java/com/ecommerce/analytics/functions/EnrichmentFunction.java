package com.ecommerce.analytics.functions;

import com.ecommerce.analytics.models.CustomerEvent;
import com.ecommerce.analytics.models.EnrichedEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EnrichmentFunction extends RichMapFunction<CustomerEvent, EnrichedEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentFunction.class);
    private static final long serialVersionUID = 1L;

    private transient ValueState<Double> customerLtvState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize state for customer LTV
        ValueStateDescriptor<Double> ltvDescriptor = new ValueStateDescriptor<>(
                "customer-ltv",
                Double.class
        );
        customerLtvState = getRuntimeContext().getState(ltvDescriptor);
    }

    @Override
    public EnrichedEvent map(CustomerEvent event) throws Exception {
        try {
            // Create enriched event from base event
            EnrichedEvent enriched = new EnrichedEvent(event);
            enriched.setProcessingTime(System.currentTimeMillis());

            // Determine user segment
            String eventType = event.getEventType();
            String userId = event.getUserId();
            if (userId != null && userId.contains("-")) {
                enriched.setUserSegment("anonymous");
            } else if ("first_purchase".equals(eventType)) {
                enriched.setUserSegment("new");
            } else if ("high_value_purchase".equals(eventType)) {
                enriched.setUserSegment("vip");
            } else if ("bulk_purchase".equals(eventType)) {
                enriched.setUserSegment("wholesale");
            } else if (eventType != null && eventType.contains("return")) {
                enriched.setUserSegment("return_customer");
            } else {
                enriched.setUserSegment("regular");
            }

            // Update customer LTV for purchase events
            updateLtv(event, enriched);

            // Calculate risk score
            enriched.setRiskScore(calculateRiskScore(event));

            return enriched;
        } catch (Exception e) {
            LOG.error("Error in enrichment: {}", e.getMessage(), e);
            // Create basic enriched event to avoid pipeline failure
            EnrichedEvent enriched = new EnrichedEvent(event);
            enriched.setUserSegment("error");
            return enriched;
        }
    }

    private void updateLtv(CustomerEvent event, EnrichedEvent enriched) throws Exception {
        String eventType = event.getEventType();

        // Update LTV for purchase events
        if (eventType != null && (eventType.contains("purchase") || eventType.contains("return"))) {
            // Get current LTV
            Double currentLtv = customerLtvState.value();
            if (currentLtv == null) {
                currentLtv = 0.0;
            }

            // Extract amount from metadata
            double amount = 0.0;
            Map<String, Object> metadata = event.getMetadata();
            if (metadata != null && metadata.containsKey("total_amount")) {
                Object amountObj = metadata.get("total_amount");

                if (amountObj instanceof Number) {
                    amount = ((Number) amountObj).doubleValue();
                } else if (amountObj instanceof String) {
                    try {
                        amount = Double.parseDouble(amountObj.toString());
                    } catch (NumberFormatException e) {
                        LOG.warn("Invalid amount format: {}", amountObj);
                    }
                }
            }

            // Calculate new LTV
            double newLtv;

            // For returns, subtract the amount (if marked as return)
            boolean isReturn = false;
            if (metadata != null && metadata.containsKey("is_return")) {
                Object isReturnObj = metadata.get("is_return");
                if (isReturnObj instanceof Boolean) {
                    isReturn = (Boolean) isReturnObj;
                } else if (isReturnObj instanceof String) {
                    isReturn = Boolean.parseBoolean(isReturnObj.toString());
                }
            }

            if (isReturn || (eventType != null && eventType.contains("return"))) {
                newLtv = currentLtv - Math.abs(amount);
            } else {
                newLtv = currentLtv + amount;
            }

            // Update state
            customerLtvState.update(newLtv);

            // Set in enriched event
            enriched.setCustomerLtv(newLtv);
        } else {
            // For non-purchase events, just use current LTV
            Double currentLtv = customerLtvState.value();
            if (currentLtv != null) {
                enriched.setCustomerLtv(currentLtv);
            }
        }
    }

    private double calculateRiskScore(CustomerEvent event) {
        double riskScore = 0.0;
        String eventType = event.getEventType();
        Map<String, Object> metadata = event.getMetadata();

        // Event type factor
        if ("high_value_purchase".equals(eventType)) {
            riskScore += 0.3;  // Higher risk for expensive purchases
        }

        if ("first_purchase".equals(eventType)) {
            riskScore += 0.2;  // Some risk for first-time buyers
        }

        // Amount factor
        if (metadata != null && metadata.containsKey("total_amount")) {
            try {
                Object amountObj = metadata.get("total_amount");
                double amount = 0.0;

                if (amountObj instanceof Number) {
                    amount = ((Number) amountObj).doubleValue();
                } else if (amountObj instanceof String) {
                    amount = Double.parseDouble(amountObj.toString());
                }

                if (amount > 500.0) {
                    riskScore += 0.5;  // High risk for large amounts
                } else if (amount > 100.0) {
                    riskScore += 0.2;  // Medium risk
                }
            } catch (Exception e) {
                // Ignore parsing errors
            }
        }

        return Math.min(riskScore, 1.0);  // Cap at 1.0
    }
}