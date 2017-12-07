package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis;

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event.LogsEvent;

/**
 * Created by jyb on 12/7/17.
 */
public interface DataDrivenAnalysisService extends EventHandler<LogsEvent> {
}
