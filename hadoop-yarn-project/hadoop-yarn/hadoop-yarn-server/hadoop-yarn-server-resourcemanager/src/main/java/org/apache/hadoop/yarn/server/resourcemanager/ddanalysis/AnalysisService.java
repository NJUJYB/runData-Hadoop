package org.apache.hadoop.yarn.server.resourcemanager.ddanalysis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ddanalysis.event.LogsEvent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by jyb on 12/7/17.
 */
public class AnalysisService extends AbstractService implements DataDrivenAnalysisService {
    private final RMContext rmContext;
    private static final Log LOG = LogFactory.getLog(AnalysisService.class);
    private File file = new File("/home/jyb/Desktop/hadoop/hadoop-2.6.2/logs/models.txt");
    private String port = ":50010";

    public AnalysisService(RMContext rmContext) {
        super(LogsService.class.getName());
        this.rmContext = rmContext;
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
    }

    @Override
    public void handle(LogsEvent event) {
        synchronized (file) {
            switch (event.getType()) {
                case ANALYSIS_REQUEST: {
                }
            }
        }
    }
}
