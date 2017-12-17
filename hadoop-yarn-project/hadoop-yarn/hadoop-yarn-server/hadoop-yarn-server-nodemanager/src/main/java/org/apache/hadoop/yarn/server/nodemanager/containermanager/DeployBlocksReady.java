package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.apache.hadoop.yarn.api.records.SplitDataInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by jyb on 12/17/17.
 */
public class DeployBlocksReady {
  private static String lockPath = "/home/jyb/Desktop/hadoop/hadoop-2.6.2/logs/locks";
  private static File fileLockPath = new File(lockPath);

  public static void createFileLock(SplitDataInfo sdi){
	try {
	  if(!fileLockPath.isDirectory()) fileLockPath.mkdirs();
	  String filename = sdi.getFilePath().replaceAll("/", "-");
	  File file = new File(lockPath + "/" + filename);
	  if(!file.exists()) file.createNewFile();

	  FileWriter writer = new FileWriter(file, true);
	  long end = sdi.getStart() + sdi.getBlockSize();
	  writer.write(sdi.getStart() + " " + end + " " + sdi.getBlockId() + "\r\n");
	  writer.close();
	}catch (IOException e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
	}
  }
}
