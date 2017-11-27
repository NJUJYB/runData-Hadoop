package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event.LogsEvent;
import org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event.ResourceAllocationLogsEvent;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by master on 17-11-26.
 */
public class LogsService extends AbstractService implements DataDrivenLogsService{
    private final RMContext rmContext;
    private static final Log LOG = LogFactory.getLog(LogsService.class);
    private File file = new File("/home/master/Desktop/hadoop/hadoop-2.6.2/logs/logs.txt");

    public LogsService(RMContext rmContext) {
        super(LogsService.class.getName());
        this.rmContext = rmContext;
    }

    @Override
    public void handle(LogsEvent event) {
        synchronized (file) {
            switch (event.getType()) {
                case RESOURCE_ADDED: {
                    ResourceAllocationLogsEvent resourceEvent = (ResourceAllocationLogsEvent) event;
                    FileWriter writer;
                    try {
                        writer = new FileWriter(file, true);
                        writer.write(resourceEvent.getResourceAllocationLogs() + "\r\n");
                        writer.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } break;
            }
        }
    }

    public ArrayList<String> getLogs() {
        synchronized (file) {
            ArrayList<String> logs = new ArrayList<String>(0);

            try {
                FileReader reader = new FileReader(file);
                BufferedReader br = new BufferedReader(reader);
                String str;
                while ((str = br.readLine()) != null) {
                    logs.add(str);
                }
                br.close();
                reader.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return logs;
        }
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void serviceStop() throws Exception {
        super.serviceStop();
    }
}
