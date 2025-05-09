package org.example;

import lombok.Data;


@Data
public class UserEvent {
    private String userId;
    private String action;
    private long timestamp;
}
