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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by master on 17-11-26.
 */
public class LogsService extends AbstractService implements DataDrivenLogsService{
    private final RMContext rmContext;
    private static final Log LOG = LogFactory.getLog(LogsService.class);
    private File file = new File("/home/jyb/Desktop/hadoop/hadoop-2.6.2/logs/logs.txt");

    private Thread eventHandlingThread;
    private final AtomicBoolean stopped;
    protected BlockingQueue<LogsEvent> eventQueue = new LinkedBlockingQueue<LogsEvent>();

    public LogsService(RMContext rmContext) {
        super(LogsService.class.getName());
        this.rmContext = rmContext;
        this.stopped = new AtomicBoolean(false);

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
    protected void serviceStart() throws Exception {
	  this.eventHandlingThread = new Thread() {
            @SuppressWarnings("unchecked")
            @Override
            public void run() {
		LogsEvent event;
		while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
		  try {
			event = LogsService.this.eventQueue.take();
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

	protected synchronized void handleEvent(LogsEvent event) {
	  switch (event.getType()) {
		case RESOURCE_ADDED: {
		  synchronized (file) {
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
			break;
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
    protected void serviceStop() throws Exception {
	  super.serviceStop();
    }
}
