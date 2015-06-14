package edu.pennstate.greatvalley.sweng.storm.topology;

import java.util.logging.Level;
import java.util.logging.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import edu.pennstate.greatvalley.sweng.storm.bolts.TweetCommentBolt;
import edu.pennstate.greatvalley.sweng.storm.bolts.TweetCommentPersistenceBolt;
import edu.pennstate.greatvalley.sweng.storm.spouts.RTSASpout;
import edu.pennstate.greatvalley.sweng.storm.utils.AppConstant;

public final class RTSATopology {
	//Creation of logger
	static Logger logger = Logger.getLogger("myLogger");
	
	private RTSATopology(){ };
	
	public static final void main(final String[] args) throws Exception {
		try {
			final Config config = new Config();
			config.setMessageTimeoutSecs(120);
			config.setDebug(false);

			TopologyBuilder topologyBuilder = new TopologyBuilder();

			String[] twitterHandles = { "@Modi", "@NarendraModi", "@PMOIndia" };

			topologyBuilder
					.setSpout("rtsaSpout", new RTSASpout(twitterHandles));
			topologyBuilder.setBolt("tweetCommentBolt", new TweetCommentBolt())
					.shuffleGrouping("rtsaSpout");
			// Create Bolt with the frequency of logging [in seconds].
			topologyBuilder.setBolt("TweetCommentPersistenceBolt",
					new TweetCommentPersistenceBolt()).shuffleGrouping(
					"tweetCommentBolt");

			// Submit it to the cluster, or submit it locally
			if (null != args && 0 < args.length) {
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config,
						topologyBuilder.createTopology());
			} else {
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(AppConstant.TOPOLOGY_NAME, config,
						topologyBuilder.createTopology());

			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}
}