package org.apache.hadoop.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * Created by jyb on 12/27/17.
 */
public class ExpLogs {
  private static File file = new File("/home/jyb/Desktop/hadoop/hadoop-2.6.2/logs/exp-logs.txt");
  private static FileWriter writer;

  public static void initial(){
	try {
	  if (!file.exists()) {
		file.createNewFile();
	  }
	} catch (IOException e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
	}
  }

  public static void writeExpLogs(String name, ArrayList<String> logs, ArrayList<Long> times){
	initial();
	try {
	  writer = new FileWriter(file, true);
	  for(int i = 0; i < logs.size(); ++i){
		//SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");
		//String timeStr = sdf.format(new Date(times.get(i)));
		//writer.write(name + ": " + timeStr + ", " + logs.get(i) + "\r\n");
		writer.write(name + ": " + times.get(i) + ", " + logs.get(i) + "\r\n");
	  }
	  writer.close();
	} catch (IOException e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
	}
  }

  public static void writeExpLogs(String name, ArrayList<String> logs){
	initial();
	try {
	  writer = new FileWriter(file, true);
	  for(String str : logs){
		writer.write(name + ": " + str + "\r\n");
	  }
	  writer.close();
	} catch (IOException e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
	}
  }
}
