package org.apache.hadoop.hdfs.server.balancer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExpLogs;

import java.io.*;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyb on 12/19/17.
 */
public class ValidBlockDeploy{
  static final Log LOG = LogFactory.getLog(ValidBlockDeploy.class);
  static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");
  static String port = ":50010";

  private long blockId, bytes;
  private String dirPath, sourceAddress, targetAddress;
  private Thread thread;
  private volatile boolean exit = false;

  public ValidBlockDeploy(long blockId, String dirPath,
	String sourceAddress, String targetAddress, long bytes) {
	this.blockId = blockId;
	this.dirPath = dirPath;
	this.sourceAddress = sourceAddress;
	this.targetAddress = targetAddress;
	this.bytes = bytes;
  }

  public void valid() {
	this.thread = new Thread() {
	  @SuppressWarnings("unchecked")
	  @Override
	  public void run() {
		while (!exit) {
		  //ArrayList<String> validLogs = new ArrayList<String>();
		  //ArrayList<Long> validTimes = new ArrayList<Long>();
		  //validLogs.add("Start connect to Namenode"); validTimes.add(System.currentTimeMillis());

		  //Connect to Namenode
		  final Configuration conf = new Configuration();
		  final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
		  List<NameNodeConnector> connectors = Collections.emptyList();
		  Socket sock = new Socket();
		  DataOutputStream out = null;
		  DataInputStream in = null;
		  Dispatcher.Source s = null;
		  Dispatcher.DDatanode.StorageGroup g = null;

		  try {
			connectors = NameNodeConnector.newNameNodeConnectors(namenodes,
					BlockDeploy.class.getSimpleName(), BALANCER_ID_PATH, conf);
			Collections.shuffle(connectors);
			NameNodeConnector nnc = connectors.get(0);

			//Get Source and Target StorageGroup
			Dispatcher dispatcher = new Dispatcher(nnc,
					Collections.<String>emptySet(), Collections.<String>emptySet(),
					5400 * 1000L, 1, 1, 1, conf);
			List<DatanodeStorageReport> reports = dispatcher.init();
			for(DatanodeStorageReport r : reports){
			  final Dispatcher.DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
			  if(r.getDatanodeInfo().getName().equals(sourceAddress + port)){
				s = dn.addSource(StorageType.DEFAULT, bytes, dispatcher);
			  }else if(r.getDatanodeInfo().getName().equals(targetAddress + port)){
				g = dn.addTarget(StorageType.DEFAULT, bytes);
			  }
			}

			//Get Block
			Dispatcher.DBlock block = null;
			final BlocksWithLocations newBlocks = nnc.getBlocks(s.getDatanodeInfo(), 2 * (1L << 30));
			for(BlocksWithLocations.BlockWithLocations blk : newBlocks.getBlocks()){
			  if(blk.getBlock().getBlockId() == blockId){
				block = new Dispatcher.DBlock(blk.getBlock());
				break;
			  }
			}
			if(block != null) {
			  //Send Request
			  sock.connect(NetUtils.createSocketAddr(g.getDatanodeInfo().getXferAddr()),
					  HdfsServerConstants.READ_TIMEOUT);
			  sock.setKeepAlive(true);
			  ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(), block.getBlock());
			  final KeyManager km = nnc.getKeyManager();
			  Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb);
			  SaslDataTransferClient saslClient = new SaslDataTransferClient(conf,
					  DataTransferSaslUtil.getSaslPropertiesResolver(conf),
					  TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);
			  IOStreamPair saslStreams = saslClient.socketSend(sock, sock.getOutputStream(),
					  sock.getInputStream(), km, accessToken, g.getDatanodeInfo());
			  in = new DataInputStream(new BufferedInputStream(saslStreams.in,
					  HdfsConstants.IO_FILE_BUFFER_SIZE));
			  out = new DataOutputStream(new BufferedOutputStream(saslStreams.out,
					  HdfsConstants.IO_FILE_BUFFER_SIZE));
			  new Sender(out).validBlock(eb, g.storageType, accessToken,
					  s.getDatanodeInfo().getDatanodeUuid(), g.getDDatanode().datanode);
			  nnc.getBytesMoved().addAndGet(block.getNumBytes());

			  //validLogs.add("End valid"); validTimes.add(System.currentTimeMillis());
			  //ExpLogs.writeExpLogs("ValidBlock", validLogs, validTimes);
			}
		  } catch (IOException e) {
			e.printStackTrace();
		  } finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
			IOUtils.closeSocket(sock);
			for(NameNodeConnector ncc : connectors) IOUtils.cleanup(LOG, ncc);
		  }
		  exit = true;
		}
	  }
	};
	this.thread.start();
  }
}
