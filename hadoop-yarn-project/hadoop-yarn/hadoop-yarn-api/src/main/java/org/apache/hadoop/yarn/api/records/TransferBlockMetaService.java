package org.apache.hadoop.yarn.api.records;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.service.AbstractService;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jyb on 12/18/17.
 */
public class TransferBlockMetaService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(TransferBlockMetaService.class);
  private Thread eventHandlingThread;
  private final AtomicBoolean stopped;
  protected BlockingQueue<SplitDataInfo> eventQueue = new LinkedBlockingQueue<SplitDataInfo>();

  public TransferBlockMetaService() {
	super(TransferBlockMetaService.class.toString());
	this.stopped = new AtomicBoolean(false);
  }

  @Override
  protected void serviceStart() throws Exception {
	this.eventHandlingThread = new Thread() {
	  @SuppressWarnings("unchecked")
	  @Override
	  public void run() {
		while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
		  try {
			SplitDataInfo sdi = TransferBlockMetaService.this.eventQueue.take();
			handleEvent(sdi);
		  } catch (InterruptedException e) {
			if (!stopped.get()) {
			  LOG.error("Returning, interrupted : " + e);
			} return;
		  }catch (Throwable t) {
			return;
		  }
		}
	  }
	};
	this.eventHandlingThread.start();
	super.serviceStart();
  }

  protected synchronized void handleEvent(SplitDataInfo sdi) {
	String metaFilePath = "/home/jyb/Desktop/hadoop/hadoop-2.6.2/hdfs/data/current";
	File dir = new File(metaFilePath);
	File[] files = dir.listFiles();
	for(File f : files){
	  if(f.isDirectory()){
		metaFilePath += "/" + f.getName() + "/current/finalized";
		break;
	  }
	}

	File subFile = DatanodeUtil.idToBlockDir(new File(metaFilePath), sdi.getBlockId());
	File[] subFiles = subFile.listFiles();
	File targetFile = null;
	for(File f : subFiles){
	  if(f.getName().indexOf(sdi.getBlockFullName() + "_") != -1){
		targetFile = f;
		break;
	  }
	}

	String cmd = "scp " + targetFile.getPath() + " jyb@" + sdi.getTargetName() + ":" + subFile.getPath();
	try {
	  Process process = Runtime.getRuntime().exec(cmd);
	  InputStreamReader ir = new InputStreamReader(process.getInputStream());
	  LineNumberReader input = new LineNumberReader(ir);
	  String line;
	  process.waitFor();
	  while((line = input.readLine()) != null){
		LOG.info(line);
	  }
	} catch (IOException e) {
	  e.printStackTrace();
	} catch (InterruptedException e) {
	  e.printStackTrace();
	}
  }

  public void handle(String info) {
	try {
	  SplitDataInfo sdi = SplitDataInfo.createNewInstanceRMToNode(info);
	  eventQueue.put(sdi);
	} catch (InterruptedException e) {
	}
  }
}
