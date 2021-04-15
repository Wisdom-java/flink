package com.ibm.vo;

public class CountByStripe {
    private String stripeNum;
    private String timeStr;
    private String hostname;
    private long timestamp;
    private int rebuild_sources_success;
    private int rebuild_sources_failed;
    private int undo_sources_success;
    private int undo_sources_failed;
    private int finalize_sources_success;
    private int finalize_sources_failed;
    private int num_rebuild_permits_available;
    private int num_rebuild_permits_used;
    private int local_rebuild_queue_length;
    private int local_rebuild_queue_drop_count;
    private int target_rebuild_rate;
    private int average_rebuild_rate;
    private int average_rebuild_length;
    private int average_rebuild_duration;
    private int total_rebuild_queue_size;
    private int global_rebuild_priority;
    private int high_priority_source_missing_slices;
    private int undo_finalize_queue_length;
    private int undo_finalize_queue_drop_count;

    public String getStripeNum() {
        return stripeNum;
    }

    public void setStripeNum(String stripeNum) {
        this.stripeNum = stripeNum;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(String timeStr) {
        this.timeStr = timeStr;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getRebuild_sources_success() {
        return rebuild_sources_success;
    }

    public void setRebuild_sources_success(int rebuild_sources_success) {
        this.rebuild_sources_success = rebuild_sources_success;
    }

    public int getRebuild_sources_failed() {
        return rebuild_sources_failed;
    }

    public void setRebuild_sources_failed(int rebuild_sources_failed) {
        this.rebuild_sources_failed = rebuild_sources_failed;
    }

    public int getUndo_sources_success() {
        return undo_sources_success;
    }

    public void setUndo_sources_success(int undo_sources_success) {
        this.undo_sources_success = undo_sources_success;
    }

    public int getUndo_sources_failed() {
        return undo_sources_failed;
    }

    public void setUndo_sources_failed(int undo_sources_failed) {
        this.undo_sources_failed = undo_sources_failed;
    }

    public int getFinalize_sources_success() {
        return finalize_sources_success;
    }

    public void setFinalize_sources_success(int finalize_sources_success) {
        this.finalize_sources_success = finalize_sources_success;
    }

    public int getFinalize_sources_failed() {
        return finalize_sources_failed;
    }

    public void setFinalize_sources_failed(int finalize_sources_failed) {
        this.finalize_sources_failed = finalize_sources_failed;
    }

    public int getNum_rebuild_permits_available() {
        return num_rebuild_permits_available;
    }

    public void setNum_rebuild_permits_available(int num_rebuild_permits_available) {
        this.num_rebuild_permits_available = num_rebuild_permits_available;
    }

    public int getNum_rebuild_permits_used() {
        return num_rebuild_permits_used;
    }

    public void setNum_rebuild_permits_used(int num_rebuild_permits_used) {
        this.num_rebuild_permits_used = num_rebuild_permits_used;
    }

    public int getLocal_rebuild_queue_length() {
        return local_rebuild_queue_length;
    }

    public void setLocal_rebuild_queue_length(int local_rebuild_queue_length) {
        this.local_rebuild_queue_length = local_rebuild_queue_length;
    }

    public int getLocal_rebuild_queue_drop_count() {
        return local_rebuild_queue_drop_count;
    }

    public void setLocal_rebuild_queue_drop_count(int local_rebuild_queue_drop_count) {
        this.local_rebuild_queue_drop_count = local_rebuild_queue_drop_count;
    }

    public int getTarget_rebuild_rate() {
        return target_rebuild_rate;
    }

    public void setTarget_rebuild_rate(int target_rebuild_rate) {
        this.target_rebuild_rate = target_rebuild_rate;
    }

    public int getAverage_rebuild_rate() {
        return average_rebuild_rate;
    }

    public void setAverage_rebuild_rate(int average_rebuild_rate) {
        this.average_rebuild_rate = average_rebuild_rate;
    }

    public int getAverage_rebuild_length() {
        return average_rebuild_length;
    }

    public void setAverage_rebuild_length(int average_rebuild_length) {
        this.average_rebuild_length = average_rebuild_length;
    }

    public int getAverage_rebuild_duration() {
        return average_rebuild_duration;
    }

    public void setAverage_rebuild_duration(int average_rebuild_duration) {
        this.average_rebuild_duration = average_rebuild_duration;
    }

    public int getTotal_rebuild_queue_size() {
        return total_rebuild_queue_size;
    }

    public void setTotal_rebuild_queue_size(int total_rebuild_queue_size) {
        this.total_rebuild_queue_size = total_rebuild_queue_size;
    }

    public int getGlobal_rebuild_priority() {
        return global_rebuild_priority;
    }

    public void setGlobal_rebuild_priority(int global_rebuild_priority) {
        this.global_rebuild_priority = global_rebuild_priority;
    }

    public int getHigh_priority_source_missing_slices() {
        return high_priority_source_missing_slices;
    }

    public void setHigh_priority_source_missing_slices(int high_priority_source_missing_slices) {
        this.high_priority_source_missing_slices = high_priority_source_missing_slices;
    }

    public int getUndo_finalize_queue_length() {
        return undo_finalize_queue_length;
    }

    public void setUndo_finalize_queue_length(int undo_finalize_queue_length) {
        this.undo_finalize_queue_length = undo_finalize_queue_length;
    }

    public int getUndo_finalize_queue_drop_count() {
        return undo_finalize_queue_drop_count;
    }

    public void setUndo_finalize_queue_drop_count(int undo_finalize_queue_drop_count) {
        this.undo_finalize_queue_drop_count = undo_finalize_queue_drop_count;
    }

    @Override
    public String toString() {
        return "CountByStripe{" +
                "stripeNum='" + stripeNum + '\'' +
                ", timeStr='" + timeStr + '\'' +
                ", hostname='" + hostname + '\'' +
                ", timestamp=" + timestamp +
                ", rebuild_sources_success=" + rebuild_sources_success +
                ", rebuild_sources_failed=" + rebuild_sources_failed +
                ", undo_sources_success=" + undo_sources_success +
                ", undo_sources_failed=" + undo_sources_failed +
                ", finalize_sources_success=" + finalize_sources_success +
                ", finalize_sources_failed=" + finalize_sources_failed +
                ", num_rebuild_permits_available=" + num_rebuild_permits_available +
                ", num_rebuild_permits_used=" + num_rebuild_permits_used +
                ", local_rebuild_queue_length=" + local_rebuild_queue_length +
                ", local_rebuild_queue_drop_count=" + local_rebuild_queue_drop_count +
                ", target_rebuild_rate=" + target_rebuild_rate +
                ", average_rebuild_rate=" + average_rebuild_rate +
                ", average_rebuild_length=" + average_rebuild_length +
                ", average_rebuild_duration=" + average_rebuild_duration +
                ", total_rebuild_queue_size=" + total_rebuild_queue_size +
                ", global_rebuild_priority=" + global_rebuild_priority +
                ", high_priority_source_missing_slices=" + high_priority_source_missing_slices +
                ", undo_finalize_queue_length=" + undo_finalize_queue_length +
                ", undo_finalize_queue_drop_count=" + undo_finalize_queue_drop_count +
                '}';
    }
}
