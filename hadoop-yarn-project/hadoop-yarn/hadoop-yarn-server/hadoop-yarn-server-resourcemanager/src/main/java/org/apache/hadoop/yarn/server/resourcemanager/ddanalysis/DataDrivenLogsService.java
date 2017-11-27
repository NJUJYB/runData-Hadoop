package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis;

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event.LogsEvent;

/**
 * Created by master on 17-11-26.
 */
public interface DataDrivenLogsService extends EventHandler<LogsEvent> {
}
