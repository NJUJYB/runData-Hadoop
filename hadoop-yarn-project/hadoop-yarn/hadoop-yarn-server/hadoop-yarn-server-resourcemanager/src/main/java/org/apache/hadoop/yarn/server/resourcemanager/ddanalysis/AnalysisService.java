package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.SplitDataInfo;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event.AnalysisRequestEvent;
import org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event.LogsEvent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancer.BlockDeploy.BlockDeployCli;

/**
 * Created by jyb on 12/7/17.
 */
public class AnalysisService extends AbstractService implements DataDrivenAnalysisService {
    private final RMContext rmContext;
    private static final Log LOG = LogFactory.getLog(AnalysisService.class);
    private File file = new File("/home/jyb/Desktop/hadoop/hadoop-2.6.2/logs/models.txt");
    private String port = ":50010";

    private Thread eventHandlingThread;
    private final AtomicBoolean stopped;
    protected BlockingQueue<LogsEvent> eventQueue = new LinkedBlockingQueue<LogsEvent>();
    private Map<String, SplitDataInfo> blocksNeededDeploy = null;
    private Map<String, SplitDataInfo> blocksNeededDeployForNode = null;

    public AnalysisService(RMContext rmContext) {
        super(LogsService.class.getName());
        this.rmContext = rmContext;
        this.stopped = new AtomicBoolean(false);

        FileWriter writer;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            writer = new FileWriter(file, true);
            writer.write("\\input jin.txt.segmented slave1\r\n");
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        blocksNeededDeploy = new HashMap<String, SplitDataInfo>();
        blocksNeededDeployForNode = new HashMap<String, SplitDataInfo>();
    }

    @Override
    protected void serviceStart() throws Exception {
        this.eventHandlingThread = new Thread() {
            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                LogsEvent event;
                while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        event = AnalysisService.this.eventQueue.take();
                    } catch (InterruptedException e) {
                        if (!stopped.get()) {
                            LOG.error("Returning, interrupted : " + e);
                        } return;
                    }
                    try {
                        handleEvent(event);
                    } catch (Throwable t) {
                        return;
                    }
                }
            }
        };
        this.eventHandlingThread.start();
        super.serviceStart();
    }

    public void clearBlocks(ApplicationId applicationId) { blocksNeededDeploy.remove(applicationId.toString()); }

    public void clearBlocksForNode(String targetName) { blocksNeededDeployForNode.remove(targetName); }

    protected synchronized void handleEvent(LogsEvent event) {
        switch (event.getType()) {
            case ANALYSIS_REQUEST: {
                String[] args = new String[4];
                AnalysisRequestEvent analysisEvent = (AnalysisRequestEvent) event;
                String applicationId = analysisEvent.getAppId();
                ArrayList<String> logs = rmContext.getLogsService().getLogs();
                for(String str: logs){
                    if(str.indexOf(applicationId) != -1){
                        if(str.indexOf("hdfs") != -1){
                            String[] splits = str.split(" ");
                            args[3] = splits[splits.length - 1].split(";")[0];
                        }
                    }
                }

                args[0] = "hdfs://master:9000/input/jin1.txt.segmented";
                args[1] = "114.212.85.99" + port;
                args[2] = "114.212.85.203" + port;
                try {
                    Tool tool = new BlockDeployCli();
                    tool.setConf(new HdfsConfiguration());
                    int r = tool.run(args);
                } catch (Exception e) {
                }
            }
        }
    }

    @Override
    public void handle(LogsEvent event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
        }
    }

    public void updateBlocks(SplitDataInfo sdi) {
        sdi.setSourceAddress("114.212.85.99");
        sdi.setSourceName("master");
        sdi.setTargetAddress("114.212.85.203");
        sdi.setTargetName("slave1");
        blocksNeededDeploy.put(sdi.getApplicationId().toString(), sdi);
        blocksNeededDeployForNode.put(sdi.getSourceName(), sdi);
    }

    public SplitDataInfo getNeededDeployBlocks(ApplicationId applicationId) {
        return blocksNeededDeploy.get(applicationId.toString());
    }

    public SplitDataInfo getNeededDeployBlocksForNode(String nodeId) {
        return blocksNeededDeployForNode.get(nodeId);
    }
}
