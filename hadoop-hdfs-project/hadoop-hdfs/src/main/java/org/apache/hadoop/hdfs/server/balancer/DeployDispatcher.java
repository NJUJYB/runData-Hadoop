package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;

import java.util.Set;

/**
 * Created by jyb on 12/6/17.
 */
public class DeployDispatcher extends Dispatcher {
    public DeployDispatcher(NameNodeConnector nnc, Set<String> includedNodes, Set<String> excludedNodes,
                            long movedWinWidth, int moverThreads, int dispatcherThreads, int maxConcurrentMovesPerNode,
                            Configuration conf) {
        super(nnc, includedNodes, excludedNodes, movedWinWidth, moverThreads, dispatcherThreads,
                maxConcurrentMovesPerNode, conf);
    }

    public void blockAdd(Source source, DDatanode.StorageGroup target,
                         String blockPath, long bytes) {
        this.blockPath = blockPath;
        this.bytes = bytes;
        add(source, target);
    }
}