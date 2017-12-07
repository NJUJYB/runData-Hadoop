package org.apache.hadoop.hdfs.server.balancer;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Source;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Task;

@InterfaceAudience.Private
public class BlockDeploy {
    static final Log LOG = LogFactory.getLog(BlockDeploy.class);

    static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");

    private static final long GB = 1L << 30; //1GB
    private static final long MAX_SIZE_TO_MOVE = 10*GB;

    private static final String USAGE = "Usage: java "
            + BlockDeploy.class.getSimpleName()
            + "\n\t[-policy <policy>]\tthe balancing policy: "
            + BalancingPolicy.Node.INSTANCE.getName() + " or "
            + BalancingPolicy.Pool.INSTANCE.getName()
            + "\n\t[-threshold <threshold>]\tPercentage of disk capacity"
            + "\n\t[-exclude [-f <hosts-file> | comma-sperated list of hosts]]"
            + "\tExcludes the specified datanodes."
            + "\n\t[-include [-f <hosts-file> | comma-sperated list of hosts]]"
            + "\tIncludes only the specified datanodes.";

    private final Dispatcher dispatcher;
    private final BalancingPolicy policy;
    private final double threshold;

    // all data node lists
    private final Collection<Source> overUtilized = new LinkedList<Source>();
    private final Collection<StorageGroup> underUtilized = new LinkedList<StorageGroup>();

    private String blockPath, src, target;
    private long bytes;

    /**
     * Construct a balancer.
     * Initialize balancer. It sets the value of the threshold, and
     * builds the communication proxies to
     * namenode as a client and a secondary namenode and retry proxies
     * when connection fails.
     */
    BlockDeploy(NameNodeConnector theblockpool, Parameters p, Configuration conf,
                String blockPath, String src, String target, long bytes) {
        final long movedWinWidth = conf.getLong(
                DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY,
                DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_DEFAULT);
        final int moverThreads = 1;
        final int dispatcherThreads = 1;
        final int maxConcurrentMovesPerNode = 1;

        this.dispatcher = new DeployDispatcher(theblockpool, p.nodesToBeIncluded,
                p.nodesToBeExcluded, movedWinWidth, moverThreads, dispatcherThreads,
                maxConcurrentMovesPerNode, conf);
        this.threshold = p.threshold; this.policy = p.policy;
        this.blockPath = blockPath; this.src = src; this.target =target;
        this.bytes = bytes;
    }

    /**
     * Given a datanode storage set, build a network topology and decide
     * over-utilized storages, above average utilized storages,
     * below average utilized storages, and underutilized storages.
     * The input datanode storage set is shuffled in order to randomize
     * to the storage matching later on.
     *
     * @return the number of bytes needed to move in order to balance the cluster.
     */
    private long init(List<DatanodeStorageReport> reports) {
        // create network topology and classify utilization collections:
        //   over-utilized, above-average, below-average and under-utilized.
        for(DatanodeStorageReport r : reports) {
            final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
            StorageType t = StorageType.DEFAULT;
            StorageGroup g = null;
            LOG.info("jyb3: " + r.getDatanodeInfo().getName());
            if(r.getDatanodeInfo().getName().equals(src)) {
                final Source s = dn.addSource(t, bytes, dispatcher);
                overUtilized.add(s);
                g = s;
            } else if(r.getDatanodeInfo().getName().equals(target)) {
                g = dn.addTarget(t, bytes);
                underUtilized.add(g);
            }
            if(g != null) dispatcher.getStorageGroupMap().put(g);
        }

        // return number of bytes to be moved in order to make the cluster balanced
        return bytes;
    }

    private static <T extends StorageGroup>
    void logUtilizationCollection(String name, Collection<T> items) {
        LOG.info(items.size() + " " + name + ": " + items);
    }

    /**
     * Decide all <source, target> pairs and
     * the number of bytes to move from a source to a target
     * Maximum bytes to be moved per storage group is
     * min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
     * @return total number of bytes to move in this iteration
     */
    private long chooseStorageGroups() {
        // First, match nodes on the same node group if cluster is node group aware
        chooseStorageGroups(overUtilized, underUtilized);

        return bytes;
    }

    /**
     * For each datanode, choose matching nodes from the candidates. Either the
     * datanodes or the candidates are source nodes with (utilization > Avg), and
     * the others are target nodes with (utilization < Avg).
     */
    private <G extends StorageGroup, C extends StorageGroup>
    void chooseStorageGroups(Collection<G> groups, Collection<C> candidates) {
        choose4One(groups.iterator().next(), candidates);
    }

    /**
     * For the given datanode, choose a candidate and then schedule it.
     * @return true if a candidate is chosen; false if no candidates is chosen.
     */
    private <C extends StorageGroup> boolean choose4One(StorageGroup g, Collection<C> candidates) {
        final Iterator<C> i = candidates.iterator();
        final C chosen = candidates.iterator().next();

        if (g instanceof Source) {
            matchSourceWithTargetToMove((Source)g, chosen);
        } else {
            matchSourceWithTargetToMove((Source)chosen, g);
        }
        return true;
    }

    private void matchSourceWithTargetToMove(Source source, StorageGroup target) {
        long size = Math.min(source.availableSizeToMove(), target.availableSizeToMove());
        final Task task = new Task(target, size);
        source.addTask(task);
        target.incScheduledSize(task.getSize());
        DeployDispatcher deployD = (DeployDispatcher) dispatcher;
        deployD.blockAdd(source, target, blockPath, bytes);
        LOG.info("jyb4: " + source.getDatanodeInfo().getName() + " " +
                target.getDatanodeInfo().getName() + " " + bytes);
        LOG.info("Decided to move "+StringUtils.byteDesc(size)+" bytes from "
                + source.getDisplayName() + " to " + target.getDisplayName());
    }

    /* reset all fields in a balancer preparing for the next iteration */
    void resetData(Configuration conf) {
        this.overUtilized.clear();
        this.underUtilized.clear();
        this.policy.reset();
        dispatcher.reset(conf);;
    }

    static class Result {
        final ExitStatus exitStatus;
        final long bytesLeftToMove;
        final long bytesBeingMoved;
        final long bytesAlreadyMoved;

        Result(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved,
               long bytesAlreadyMoved) {
            this.exitStatus = exitStatus;
            this.bytesLeftToMove = bytesLeftToMove;
            this.bytesBeingMoved = bytesBeingMoved;
            this.bytesAlreadyMoved = bytesAlreadyMoved;
        }

        void print(PrintStream out) {
            out.printf("%-24s %19s  %18s  %17s%n",
                    DateFormat.getDateTimeInstance().format(new Date()),
                    StringUtils.byteDesc(bytesAlreadyMoved),
                    StringUtils.byteDesc(bytesLeftToMove),
                    StringUtils.byteDesc(bytesBeingMoved));
        }
    }

    Result newResult(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved) {
        return new Result(exitStatus, bytesLeftToMove, bytesBeingMoved,
                dispatcher.getBytesMoved());
    }

    Result newResult(ExitStatus exitStatus) {
        return new Result(exitStatus, -1, -1, dispatcher.getBytesMoved());
    }

    /** Run an iteration for all datanodes. */
    Result runOneIteration() {
        LOG.info("jyb2: " + blockPath + " " + src + " " + target + " " + bytes);
        try {
            final List<DatanodeStorageReport> reports = dispatcher.init();
            final long bytesLeftToMove = init(reports);
            final long bytesBeingMoved = chooseStorageGroups();
            if (!dispatcher.dispatchAndCheckContinue()) {
                return newResult(ExitStatus.NO_MOVE_PROGRESS, bytes, bytesBeingMoved);
            }
            return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingMoved);
        } catch (IllegalArgumentException e) {
            System.out.println(e + ".  Exiting ...");
            return newResult(ExitStatus.ILLEGAL_ARGUMENTS);
        } catch (IOException e) {
            System.out.println(e + ".  Exiting ...");
            return newResult(ExitStatus.IO_EXCEPTION);
        } catch (InterruptedException e) {
            System.out.println(e + ".  Exiting ...");
            return newResult(ExitStatus.INTERRUPTED);
        } finally {
            dispatcher.shutdownNow();
        }
    }

    /**
     * Balance all namenodes.
     * For each iteration,
     * for each namenode,
     * execute a {@link Balancer} to work through all datanodes once.
     */
    static int run(Collection<URI> namenodes, final Parameters p, Configuration conf,
                   String blockPath, String src, String target, long bytes) throws IOException, InterruptedException {
        final long sleeptime = 1000;
        LOG.info("namenodes  = " + namenodes);
        LOG.info("parameters = " + p);

        System.out.println("Time Stamp Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");

        List<NameNodeConnector> connectors = Collections.emptyList();
        try {
            connectors = NameNodeConnector.newNameNodeConnectors(namenodes,
                    BlockDeploy.class.getSimpleName(), BALANCER_ID_PATH, conf);

            boolean done = true;
            Collections.shuffle(connectors);
            for(NameNodeConnector nnc : connectors) {
                final BlockDeploy b = new BlockDeploy(nnc, p, conf, blockPath, src, target, bytes);
                final Result r = b.runOneIteration();
                r.print(System.out);

                // clean all lists
                b.resetData(conf);
                if (r.exitStatus == ExitStatus.IN_PROGRESS) {
                    done = false;
                } else if (r.exitStatus != ExitStatus.SUCCESS) {
                    //must be an error statue, return.
                    return r.exitStatus.getExitCode();
                }
                break;
            }

            while (!done) {
                Thread.sleep(sleeptime);
            }
        } finally {
            for(NameNodeConnector nnc : connectors) {
                IOUtils.cleanup(LOG, nnc);
            }
        }
        return ExitStatus.SUCCESS.getExitCode();
    }

    static class Parameters {
        static final Parameters DEFAULT = new Parameters(
                BalancingPolicy.Node.INSTANCE, 10.0,
                Collections.<String> emptySet(), Collections.<String> emptySet());

        final BalancingPolicy policy;
        final double threshold;
        // exclude the nodes in this set from balancing operations
        Set<String> nodesToBeExcluded;
        //include only these nodes in balancing operations
        Set<String> nodesToBeIncluded;

        Parameters(BalancingPolicy policy, double threshold,
                   Set<String> nodesToBeExcluded, Set<String> nodesToBeIncluded) {
            this.policy = policy;
            this.threshold = threshold;
            this.nodesToBeExcluded = nodesToBeExcluded;
            this.nodesToBeIncluded = nodesToBeIncluded;
        }

        @Override
        public String toString() {
            return Balancer.class.getSimpleName() + "." + getClass().getSimpleName()
                    + "[" + policy + ", threshold=" + threshold +
                    ", number of nodes to be excluded = "+ nodesToBeExcluded.size() +
                    ", number of nodes to be included = "+ nodesToBeIncluded.size() +"]";
        }
    }

    public static class BlockDeployCli extends Configured implements Tool {
        /**
         * Parse arguments and then run Balancer.
         *
         * @param args command specific arguments.
         * @return exit code. 0 indicates success, non-zero indicates failure.
         */
        @Override
        public int run(String[] args) {
            //String blockPath, String src, String target, long bytes
            final Configuration conf = getConf();

            LOG.info("jyb1: " + args[0] + " " + args[1] + " " + args[2] + " " + args[3]);
            try {
                final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
                return BlockDeploy.run(namenodes, parse(), conf, args[0], args[1], args[2], Long.parseLong(args[3]));
            } catch (IOException e) {
                System.out.println(e + ".  Exiting ...");
                return ExitStatus.IO_EXCEPTION.getExitCode();
            } catch (InterruptedException e) {
                System.out.println(e + ".  Exiting ...");
                return ExitStatus.INTERRUPTED.getExitCode();
            } finally {
                System.out.format("%-24s ", DateFormat.getDateTimeInstance().format(new Date()));
            }
        }

        /** parse command line arguments */
        static Parameters parse() {
            BalancingPolicy policy = Parameters.DEFAULT.policy;
            double threshold = Parameters.DEFAULT.threshold;
            Set<String> nodesTobeExcluded = Parameters.DEFAULT.nodesToBeExcluded;
            Set<String> nodesTobeIncluded = Parameters.DEFAULT.nodesToBeIncluded;
            return new Parameters(policy, threshold, nodesTobeExcluded, nodesTobeIncluded);
        }
    }
}