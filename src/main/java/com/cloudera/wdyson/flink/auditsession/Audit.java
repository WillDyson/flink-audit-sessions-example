package com.cloudera.wdyson.flink.auditsession;

import java.sql.Timestamp;
import java.util.Optional;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Audit {
    private static final Logger LOG = LoggerFactory.getLogger(Audit.class);

    private static ObjectMapper om  = new ObjectMapper();

    public int repoType;
    public String repo;
    public String reqUser;
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    public Timestamp evtTime;
    public String action;
    public String access;
    public String resource;
    public String resType;
    public int result;
    public String agent;
    public int policy;
    public int policy_version;
    public String enforcer;
    public String cliIP;
    public String reqData;
    public String agentHost;
    public String logType;
    public String id;
    public int seq_num;
    public int event_count;
    public int event_dur_ms;
    public String[] tags;
    public String cluster_name;

    public static Optional<Audit> fromJson(String json) {
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            return Optional.ofNullable(om.readValue(json, Audit.class));
        } catch (JsonProcessingException e) {
            LOG.info(e.getMessage());

            return Optional.empty();
        }
    }
}
