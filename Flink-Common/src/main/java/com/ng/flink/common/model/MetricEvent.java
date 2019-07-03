package com.ng.flink.common.model;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
/**
 * @Author: Cedaris
 * @Date: 2019/6/20 16:03
 */
public class MetricEvent {
    /**
     * Metric name
     */
    private String name;

    /**
     * Metric timestamp
     */
    private Long timestamp;

    /**
     * Metric fields
     */
    private Map<String, Object> fields;

    /**
     * Metric tags
     */
    private Map<String, String> tags;

    public long getTimestamp() {
        long currentTimeMillis = System.currentTimeMillis();
        return currentTimeMillis;
    }
}
