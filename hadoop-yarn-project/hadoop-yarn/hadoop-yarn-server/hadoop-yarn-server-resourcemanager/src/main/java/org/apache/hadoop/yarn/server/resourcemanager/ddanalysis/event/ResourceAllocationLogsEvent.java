package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event;

/**
 * Created by master on 17-11-26.
 */
public class ResourceAllocationLogsEvent extends LogsEvent {
    private String data;

    public ResourceAllocationLogsEvent(String resources) {
        super(LogsEventType.RESOURCE_ADDED);
        this.data = resources;
    }

    public String getResourceAllocationLogs() {
        return data;
    }
}
