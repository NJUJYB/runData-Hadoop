package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

/**
 * Created by jyb on 12/18/17.
 */
public class ExtraBlockOps {
  private static final Log LOG = LogFactory.getLog(ExtraBlockOps.class);

  public ExtraBlockOps() {
  }

  public void deleteFileLock(String filePath){
	String cmd = "rm " + filePath;
	exeCommand(cmd);
  }

  public void moveBlockToHDFSPath(String filePath, long blockId){
	String cmd = "mv " + filePath + " ";
	String path = searchForBlock("/home/jyb/Desktop/hadoop/hadoop-2.6.2/hdfs/data/current", blockId);
	if(path != null) {
	  cmd = cmd + path.split("blk_" + blockId + "_")[0];
	  exeCommand(cmd);
	}
  }

  private String searchForBlock(String path, long blockId) {
	File dir = new File(path);
	if(dir.isDirectory()){
	  File[] files =dir.listFiles();
	  for(File f : files){
		String resPath = searchForBlock(f.getPath(), blockId);
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
