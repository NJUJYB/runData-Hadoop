package org.apache.hadoop.mapreduce.lib.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.balancer.ValidBlockDeploy;

import java.io.*;

/**
 * Created by jyb on 12/18/17.
 */
public class ExtraBlockOps {
  private static final Log LOG = LogFactory.getLog(ExtraBlockOps.class);
  private String sourceAddress, targetAddress;
  private long blockId, bytes;

  public ExtraBlockOps(String sourceAddress, String targetAddress,
	long blockId, long bytes) {
	this.sourceAddress = sourceAddress;
	this.targetAddress = targetAddress;
	this.blockId = blockId;
	this.bytes = bytes;
  }

  public void deleteFileLock(String filePath){
	String cmd = "rm " + filePath;
	exeCommand(cmd);
  }

  public void moveBlockToHDFSPath(String filePath){
	String path = searchForBlock("/home/jyb/Desktop/hadoop/hadoop-2.6.2/hdfs/data/current");

	File file = new File("/home/jyb/Desktop/hadoop/hadoop-2.6.2/logs/locks/" + blockId);
	if(!file.exists()){
	  try {
		file.createNewFile();
		FileWriter writer = new FileWriter(file, true);
		writer.write(filePath + "\r\n");
		writer.write(path.split("blk_" + blockId + "_")[0] + "\r\n");
		writer.close();
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	}

	if(path != null) {
	  ValidBlockDeploy vbd = new ValidBlockDeploy(blockId,
			  path.split("blk_" + blockId + "_")[0], sourceAddress, targetAddress, bytes);
	  vbd.valid();
	}
  }

  private String searchForBlock(String path) {
	File dir = new File(path);
	if(dir.isDirectory()){
	  File[] files =dir.listFiles();
	  for(File f : files){
		String resPath = searchForBlock(f.getPath());
		if(resPath != null) return resPath;
		else continue;
	  }
	} else {
	  if(dir.getName().indexOf("blk_" + blockId + "_") != -1) return dir.getPath();
	  else return null;
	}
	return null;
  }

  public void exeCommand(String cmd){
	LOG.info(cmd);
	try {
	  Process process = Runtime.getRuntime().exec(cmd);
	  LineNumberReader input = new LineNumberReader(
			  new InputStreamReader(process.getInputStream()));
	  String line;
	  process.waitFor();
	  while((line = input.readLine()) != null){ LOG.info(line); }
	} catch (Exception e) {
	  e.printStackTrace();
	}
  }
}
