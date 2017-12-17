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

package org.apache.hadoop.mapreduce.split;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.records.SplitDataInfo;

/**
 * A utility that reads the split meta info and creates
 * split meta info objects
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SplitMetaInfoReader {
  
  public static JobSplit.TaskSplitMetaInfo[] readSplitMetaInfo(
      JobID jobId, FileSystem fs, Configuration conf, Path jobSubmitDir) 
  throws IOException {
    long maxMetaInfoSize = conf.getLong(MRJobConfig.SPLIT_METAINFO_MAXSIZE,
        MRJobConfig.DEFAULT_SPLIT_METAINFO_MAXSIZE);
    Path metaSplitFile = JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir);
    String jobSplitFile = JobSubmissionFiles.getJobSplitFile(jobSubmitDir).toString();
    FileStatus fStatus = fs.getFileStatus(metaSplitFile);
    if (maxMetaInfoSize > 0 && fStatus.getLen() > maxMetaInfoSize) {
      throw new IOException("Split metadata size exceeded " +
          maxMetaInfoSize +". Aborting job " + jobId);
    }
    FSDataInputStream in = fs.open(metaSplitFile);
    byte[] header = new byte[JobSplit.META_SPLIT_FILE_HEADER.length];
    in.readFully(header);
    if (!Arrays.equals(JobSplit.META_SPLIT_FILE_HEADER, header)) {
      throw new IOException("Invalid header on split file");
    }
    int vers = WritableUtils.readVInt(in);
    if (vers != JobSplit.META_SPLIT_VERSION) {
      in.close();
      throw new IOException("Unsupported split version " + vers);
    }
    int numSplits = WritableUtils.readVInt(in); //TODO: check for insane values
    JobSplit.TaskSplitMetaInfo[] allSplitMetaInfo = 
      new JobSplit.TaskSplitMetaInfo[numSplits];

    Map<String, SplitDataInfo> splitPathMap = new HashMap<String, SplitDataInfo>(0);
    FSDataInputStream splitFile = fs.open(JobSubmissionFiles.getJobSplitFile(jobSubmitDir));
    splitFile.read(new byte["SPL".getBytes("UTF-8").length]);
    splitFile.readInt();
    for(int i = 0; i < numSplits; ++i) {
      try {
        long offset = splitFile.getPos();
        int classNameLen = WritableUtils.readVInt(splitFile);
        splitFile.read(new byte[classNameLen]);
        int pathLen = WritableUtils.readVInt(splitFile);
        byte[] pathNameByte = new byte[pathLen];
        splitFile.read(pathNameByte);
        String pathName = new String(pathNameByte);
        long start = splitFile.readLong();
        long length = splitFile.readLong();
        SplitDataInfo sdi = new SplitDataInfo(pathName, start, length);
        splitPathMap.put("" + offset, sdi);
      } catch (EOFException e) {}
    }
    splitFile.close();


    for (int i = 0; i < numSplits; i++) {
      JobSplit.SplitMetaInfo splitMetaInfo = new JobSplit.SplitMetaInfo();
      splitMetaInfo.readFields(in);
      SplitDataInfo sdi = splitPathMap.get("" + splitMetaInfo.getStartOffset());
      JobSplit.TaskSplitIndex splitIndex = new JobSplit.TaskSplitIndex(
          jobSplitFile, 
          splitMetaInfo.getStartOffset(), sdi);
      allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex, 
          splitMetaInfo.getLocations(), 
          splitMetaInfo.getInputDataLength());
    }
    in.close();

    return allSplitMetaInfo;
  }
}
