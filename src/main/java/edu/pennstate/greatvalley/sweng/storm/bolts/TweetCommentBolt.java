package edu.pennstate.greatvalley.sweng.storm.bolts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.pennstate.greatvalley.sweng.storm.domain.TweetComment;
import edu.pennstate.greatvalley.sweng.storm.sentiment.AfinnResource;

public class TweetCommentBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private OutputCollector outputCollector;
	private TopologyContext context;

	private Set<String> affinWords;

	public void prepare(Map map, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector = collector;
		this.context = context;
		affinWords = AfinnResource.getAfinnWords();
		// or any property file reading with regards to key
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweetComment"));
	}

	public void execute(Tuple tuple) {

		final Status status = (Status) tuple.getValueByField("tweet");

		String tweetText = status.getText();

		Map<String, Integer> sentimentWordCountMap = getSentimentWordAndCount(tweetText);

		long tweetID = status.getId();

		User user = status.getUser();

		String screenName = user.getScreenName();
		String userName = user.getName();
		Date tweetTime = status.getCreatedAt();
		GeoLocation geoLocation = status.getGeoLocation();

		TweetComment tweetFeed = new TweetComment();

		tweetFeed.setTweetID(tweetID);
		tweetFeed.setUserName(userName);
		tweetFeed.setScrenName(screenName);
		tweetFeed.setTweetText(tweetText);
		tweetFeed.setTweetTime(tweetTime);

		tweetFeed.setSource(status.getSource());

		tweetFeed.setSentimentWordCountMap(sentimentWordCountMap);

		System.out.println(tweetFeed);

		outputCollector.emit(new Values(tweetFeed));

	}

	private Map<String, Integer> getSentimentWordAndCount(String comment) {

		Map<String, Integer> sentiemtWordMap = new HashMap<String, Integer>();

		List<String> listOfCommentWords = getListOfCommentWordsHavingLengthGreaterThenSmallestAfinnWord(comment);

		for (String str : listOfCommentWords) {
			if (affinWords.contains(str)) {
				if (sentiemtWordMap.containsKey(str)) {
					int count = sentiemtWordMap.get(str);
					sentiemtWordMap.put(str, count++);
				} else {
					sentiemtWordMap.put(str, 1);
				}
			}
		}

		return sentiemtWordMap;
	}

	private List<String> getListOfCommentWordsHavingLengthGreaterThenSmallestAfinnWord(
			String comment) {
		List<String> listOfFilterdWords = new ArrayList<String>();

		int lengthOfSmallestAffinWord = AfinnResource
				.getLengthOfSmallestAffinWord();

		String regEx = "[\\s,\",']";

		String[] split = comment.split(regEx);

		List<String> allWords = Arrays.asList(split);

		for (String str : allWords) {
			if (str.trim().length() > lengthOfSmallestAffinWord) {
				listOfFilterdWords.add(str.trim());
			}
		}

		return listOfFilterdWords;
	}

	public static void main(String[] args) {
		String str = "one,two,three,four,\"five1,five2\", 	six ,seven,\"eight1,ei'ght2\",\"nine\",,eleven";

		String delim = "[\\s,\",']";
		String[] split = str.split(delim);

		System.out.println(Arrays.asList(split));

	}
}
