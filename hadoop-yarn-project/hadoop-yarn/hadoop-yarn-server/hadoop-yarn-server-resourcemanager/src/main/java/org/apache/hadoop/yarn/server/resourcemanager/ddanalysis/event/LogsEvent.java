package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Created by master on 17-11-26.
 */
public class LogsEvent extends AbstractEvent<LogsEventType> {
    public LogsEvent(LogsEventType type) {
        super(type);
    }
}
