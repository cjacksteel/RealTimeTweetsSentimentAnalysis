package edu.pennstate.greatvalley.sweng.storm.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import edu.pennstate.greatvalley.sweng.storm.bolts.TweetCommentBolt;
import edu.pennstate.greatvalley.sweng.storm.bolts.TweetCommentPersistenceBolt;
import edu.pennstate.greatvalley.sweng.storm.spouts.RTSASpout;
import edu.pennstate.greatvalley.sweng.storm.utils.AppConstant;

public final class RTSATopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(RTSATopology.class);

	public static final void main(final String[] args) throws Exception {
		try {
			final Config config = new Config();
			config.setMessageTimeoutSecs(120);
			config.setDebug(false);

			TopologyBuilder topologyBuilder = new TopologyBuilder();

			String[] twitterHandles = { "@Modi", "@NarendraModi", "@PMOIndia" };

			// String[] twitterHandles = {"penn state", "@penn_state",
			// "@PennStateFball", "@GoPSUSports",
			// "@PennStateMHKY","@pennstatenews","@PennStateMBB","@psufootball","@PennStateAlums","@PSUWorldCampus","@PennStateBase"};

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
		} catch (Exception exception) {

			exception.printStackTrace();
		}

	}
}