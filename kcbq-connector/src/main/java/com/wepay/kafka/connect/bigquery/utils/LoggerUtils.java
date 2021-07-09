package com.wepay.kafka.connect.bigquery.utils;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

public class LoggerUtils {

    public static void logRecord(Set<SinkRecord> rows, Logger logger) {
        AtomicInteger cpt = new AtomicInteger(0);
        rows.forEach(sinkRow -> logger.error("[index {}] SinkRow(t:{}/p:{}/o:{}/t:{}) - schema(id:{})", cpt.getAndIncrement(), sinkRow.topic(),
                sinkRow.kafkaPartition(), sinkRow.kafkaOffset(), sinkRow.timestamp(), sinkRow.valueSchema().version()));
    }
}
