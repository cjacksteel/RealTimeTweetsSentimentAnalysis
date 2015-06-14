package edu.pennstate.greatvalley.sweng.storm.bolts;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.pennstate.greatvalley.sweng.storm.domain.TweetComment

public class TweetCommentPersistenceBolt extends BaseRichBolt {
	//Creation of logger
	static Logger logger = Logger.getLogger("myLogger");

	private static final long serialVersionUID = 1L;

	private OutputCollector outputCollector;
	private TopologyContext context;
	private MongoOperations mongoOperation;
	private final String collectionName = "TweetComment";

	
	public void prepare(Map map, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		this.context = context;

		ApplicationContext springContext = new ClassPathXmlApplicationContext(
				"Spring-Config.xml");
		mongoOperation = (MongoOperations) springContext
				.getBean("mongoTemplate");

	

		// or any property file reading with regards to key
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweetCommentPersitent"));
	}

	public void execute(Tuple tuple) {

		TweetComment tweetFeed = (TweetComment) tuple
				.getValueByField("tweetComment");

	logger.log(Level.INFO, "TweetCommentPersistenceBolt->tweetFeed "
				+ tweetFeed);
		
		mongoOperation.save(tweetFeed, collectionName);

	}

	

	
}
