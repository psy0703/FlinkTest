package com.ng.flink.common.watermarks;
import com.ng.flink.common.model.MetricEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MetricWatermark implements AssignerWithPeriodicWatermarks<MetricEvent> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(MetricEvent metricEvent, long previousElementTimestamp) {

        if (metricEvent.getTimestamp() > this.currentTimestamp) {
            this.currentTimestamp = metricEvent.getTimestamp();
        }
        return this.currentTimestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);

    }
}
