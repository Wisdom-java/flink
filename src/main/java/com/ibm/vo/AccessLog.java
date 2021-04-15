package com.ibm.vo;

public class AccessLog {
    private String vault_name;
    private String server_name;
    private String client_id;
    private String user_id;
    private String remote_address;
    private String request_method;
    private String protocol;
    private String object_length;
    private String object_name;
    private String time_start;
    private String time_finish;
    private String user_agent;
    private String status;
    private String midstream_error;
    private String error_code;
    private String request_latency;
    private String response_length;
    private String turn_around_time;
    private String error_message;
    private long timestamp_finish;

    public String getVault_name() {
        return vault_name;
    }

    public String getServer_name() {
        return server_name;
    }

    public String getClient_id() {
        return client_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public String getRemote_address() {
        return remote_address;
    }

    public String getRequest_method() {
        return request_method;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getObject_length() {
        return object_length;
    }

    public String getObject_name() {
        return object_name;
    }

    public String getTime_start() {
        return time_start;
    }

    public String getTime_finish() {
        return time_finish;
    }

    public String getUser_agent() {
        return user_agent;
    }

    public String getStatus() {
        return status;
    }

    public String getMidstream_error() {
        return midstream_error;
    }

    public String getError_code() {
        return error_code;
    }

    public String getRequest_latency() {
        return request_latency;
    }

    public String getResponse_length() {
        return response_length;
    }

    public String getTurn_around_time() {
        return turn_around_time;
    }

    public String getError_message() {
        return error_message;
    }

    public long getTimestamp_finish() {
        return timestamp_finish;
    }

    public void setVault_name(String vault_name) {
        this.vault_name = vault_name;
    }

    public void setServer_name(String server_name) {
        this.server_name = server_name;
    }

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public void setRemote_address(String remote_address) {
        this.remote_address = remote_address;
    }

    public void setRequest_method(String request_method) {
        this.request_method = request_method;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setObject_length(String object_length) {
        this.object_length = object_length;
    }

    public void setObject_name(String object_name) {
        this.object_name = object_name;
    }

    public void setTime_start(String time_start) {
        this.time_start = time_start;
    }

    public void setTime_finish(String time_finish) {
        this.time_finish = time_finish;
    }

    public void setUser_agent(String user_agent) {
        this.user_agent = user_agent;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setMidstream_error(String midstream_error) {
        this.midstream_error = midstream_error;
    }

    public void setError_code(String error_code) {
        this.error_code = error_code;
    }

    public void setRequest_latency(String request_latency) {
        this.request_latency = request_latency;
    }

    public void setResponse_length(String response_length) {
        this.response_length = response_length;
    }

    public void setTurn_around_time(String turn_around_time) {
        this.turn_around_time = turn_around_time;
    }

    public void setError_message(String error_message) {
        this.error_message = error_message;
    }

    public void setTimestamp_finish(long timestamp_finish) {
        this.timestamp_finish = timestamp_finish;
    }

    @Override
    public String toString() {
        return  vault_name + ","
                + server_name + ","
                + client_id + ","
                + user_id + ","
                + remote_address + ","
                + request_method + ","
                + protocol + ","
                + object_length + ","
                + object_name + ","
                + time_start + ","
                + time_finish + ","
                + user_agent + ","
                + status + ","
                + midstream_error + ","
                + error_code + ","
                + request_latency + ","
                + response_length + ","
                + turn_around_time + ","
                + error_message;

    }
}
