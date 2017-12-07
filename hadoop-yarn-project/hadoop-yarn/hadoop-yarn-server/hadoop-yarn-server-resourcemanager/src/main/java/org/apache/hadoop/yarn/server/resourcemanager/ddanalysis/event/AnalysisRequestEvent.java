package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event;

/**
 * Created by jyb on 12/7/17.
 */
public class AnalysisRequestEvent extends LogsEvent {
    private String appId;

    public AnalysisRequestEvent(String appId) {
        super(LogsEventType.ANALYSIS_REQUEST);
        this.appId = appId;
    }

    public String getAppId() { return appId; }
}
