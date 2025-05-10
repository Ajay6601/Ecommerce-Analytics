// MongoDB initialization script
db = db.getSiblingDB('ecommerce_analytics');

// Create collections with schema validation
db.createCollection('processed_events', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['event_id', 'event_type', 'user_id', 'timestamp'],
            properties: {
                event_id: {
                    bsonType: 'string',
                    description: 'Unique identifier for the event'
                },
                event_type: {
                    bsonType: 'string',
                    description: 'Type of event (purchase, page_view, etc.)'
                },
                user_id: {
                    bsonType: 'string',
                    description: 'Identifier for the user'
                },
                timestamp: {
                    bsonType: 'string',
                    description: 'ISO-8601 formatted timestamp'
                },
                processing_time: {
                    bsonType: 'number',
                    description: 'Processing timestamp'
                },
                user_segment: {
                    bsonType: 'string',
                    description: 'User segment'
                }
            }
        }
    }
});

db.createCollection('hourly_revenue', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['hour_timestamp', 'total_revenue', 'order_count'],
            properties: {
                hour_timestamp: {
                    bsonType: 'string',
                    description: 'Hour timestamp in ISO format'
                },
                total_revenue: {
                    bsonType: 'number',
                    description: 'Total revenue for the hour'
                },
                order_count: {
                    bsonType: 'int',
                    description: 'Number of orders in the hour'
                },
                unique_customers: {
                    bsonType: 'int',
                    description: 'Number of unique customers'
                }
            }
        }
    }
});

db.createCollection('popular_products', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['product_id', 'product_name', 'total_quantity'],
            properties: {
                product_id: {
                    bsonType: 'string',
                    description: 'Product identifier'
                },
                product_name: {
                    bsonType: 'string',
                    description: 'Product name'
                },
                total_quantity: {
                    bsonType: 'int',
                    description: 'Total quantity sold'
                },
                total_revenue: {
                    bsonType: 'number',
                    description: 'Total revenue generated'
                }
            }
        }
    }
});

db.createCollection('product_recommendations', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['product_id'],
            properties: {
                product_id: {
                    bsonType: 'string',
                    description: 'Product identifier'
                },
                product_name: {
                    bsonType: 'string',
                    description: 'Product name'
                },
                recommendations: {
                    bsonType: 'array',
                    description: 'Recommended products',
                    items: {
                        bsonType: 'object',
                        required: ['product_id', 'similarity_score'],
                        properties: {
                            product_id: {
                                bsonType: 'string',
                                description: 'Recommended product ID'
                            },
                            product_name: {
                                bsonType: 'string',
                                description: 'Recommended product name'
                            },
                            similarity_score: {
                                bsonType: 'number',
                                description: 'Similarity score'
                            }
                        }
                    }
                },
                updated_at: {
                    bsonType: 'string',
                    description: 'Last update timestamp'
                }
            }
        }
    }
});

// Create indexes
db.processed_events.createIndex({ "event_id": 1 }, { unique: true });
db.processed_events.createIndex({ "user_id": 1 });
db.processed_events.createIndex({ "timestamp": 1 });
db.processed_events.createIndex({ "event_type": 1 });

db.hourly_revenue.createIndex({ "hour_timestamp": 1 }, { unique: true });

db.popular_products.createIndex({ "product_id": 1 }, { unique: true });
db.popular_products.createIndex({ "total_quantity": -1 });

db.product_recommendations.createIndex({ "product_id": 1 }, { unique: true });

print("MongoDB initialized successfully");