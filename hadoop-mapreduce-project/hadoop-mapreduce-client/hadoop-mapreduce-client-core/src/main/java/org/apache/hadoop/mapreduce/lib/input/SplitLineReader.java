/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExpLogs;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SplitLineReader extends org.apache.hadoop.util.LineReader {
  private ExtraBlockOps ops;

  public SplitLineReader(InputStream in, byte[] recordDelimiterBytes) {
    super(in, recordDelimiterBytes);
  }

  public SplitLineReader(InputStream in, Configuration conf,
      byte[] recordDelimiterBytes,
      String filePath, long start, long end) throws IOException {
    super(in, conf, recordDelimiterBytes, filePath, start, end);
    if(fileLock != null && fileLock.exists() && blockId != 0L){
      ops = new ExtraBlockOps(sourceAddress, targetAddress, blockId, bytes);
      ops.deleteFileLock(fileLock.getPath());
    }
  }

  @Override
  public void close() throws IOException {
    super.close();

    Thread thread = new Thread(){
      @SuppressWarnings("unchecked")
      @Override
      public void run() {
        if(blockId != 0L){
          //ArrayList<String> copyToDiskLogs = new ArrayList<String>();
          //ArrayList<Long> copyToDiskTimes = new ArrayList<Long>();
          //copyToDiskLogs.add("Start CopyToDist"); copyToDiskTimes.add(System.nanoTime());

          String targetFilePath = "/home/jyb/Desktop/hadoop/hadoop-2.6.2/logs/blockCache/blk_" + blockId;
          File file = new File(targetFilePath);
          try{
            if(!file.exists()) {
              file.createNewFile();
              FileOutputStream fos = new FileOutputStream(file, true);
              fos.write(splitBuf);
              fos.flush();
              fos.close();
              splitBuf = null;
            }
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          //copyToDiskLogs.add("End CopyToDist"); copyToDiskTimes.add(System.nanoTime());
          //ExpLogs.writeExpLogs("BufferCopyToDisk", copyToDiskLogs, copyToDiskTimes);

          ops.moveBlockToHDFSPath(targetFilePath);
          blockId = 0L;
        }
        if(expLogs != null) ExpLogs.writeExpLogs("FillBuffer", expLogs, times);
      }
    };
    thread.start();
  }

  public SplitLineReader(InputStream in, Configuration conf,
                         byte[] recordDelimiterBytes) throws IOException {
    super(in, conf, recordDelimiterBytes);
  }

  public boolean needAdditionalRecordAfterSplit() {
    return false;
  }
}
