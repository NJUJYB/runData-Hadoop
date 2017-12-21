package org.apache.hadoop.mapreduce.split;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.yarn.api.records.SplitDataInfo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jyb on 12/16/17.
 */
public class FileToBlockInfo {
  private static final Log LOG = LogFactory.getLog(FileToBlockInfo.class);
  private long blockId = 0L, blockSize = 0L;

  public FileToBlockInfo(SplitDataInfo sdi) {
	Configuration conf = new Configuration();
	try {
	  DFSClient dfsClient = new DFSClient(new URI("hdfs://114.212.85.99:9000"), conf);
	  LocatedBlocks blocks
			  = dfsClient.getLocatedBlocks(sdi.getFilePath().split("master:9000")[1],
			  sdi.getStart(), sdi.getNeededLength());
	  for(LocatedBlock b : blocks.getLocatedBlocks()){
		if(blockId == 0L) {
		  blockId = b.getBlock().getBlockId();
		  blockSize = b.getBlockSize();
		  break;
		}
	  }
	  dfsClient.close();
	} catch (IOException e) {
	  e.printStackTrace();
	} catch (URISyntaxException e) {
	  e.printStackTrace();
	}
  }

  public long getBlockId() { return blockId; }

  public long getBlockSize() { return blockSize; }
}
