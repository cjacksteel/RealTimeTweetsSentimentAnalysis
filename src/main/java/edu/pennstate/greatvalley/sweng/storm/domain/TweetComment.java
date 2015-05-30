package edu.pennstate.greatvalley.sweng.storm.domain;

import java.util.Date;
import java.util.Map;

public class TweetComment {

	private long tweetID;
	private String userName;
	private String screnName;
	private double lattitude;
	private double longitude;
	private String source;
	private String tweetText;
	private Date tweetTime;
	private Map<String, Integer> sentimentWordCountMap;

	public long getTweetID() {
		return tweetID;
	}

	public void setTweetID(long tweetID) {
		this.tweetID = tweetID;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getScrenName() {
		return screnName;
	}

	public void setScrenName(String screnName) {
		this.screnName = screnName;
	}


	public String getTweetText() {
		return tweetText;
	}

	public void setTweetText(String tweetText) {
		this.tweetText = tweetText;
	}

	public Date getTweetTime() {
		return tweetTime;
	}

	public void setTweetTime(Date tweetTime) {
		this.tweetTime = tweetTime;
	}

	
	public double getLattitude() {
		return lattitude;
	}

	public void setLattitude(double lattitude) {
		this.lattitude = lattitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	
	public Map<String, Integer> getSentimentWordCountMap() {
		return sentimentWordCountMap;
	}

	public void setSentimentWordCountMap(Map<String, Integer> sentimentWordCountMap) {
		this.sentimentWordCountMap = sentimentWordCountMap;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (tweetID ^ (tweetID >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TweetComment other = (TweetComment) obj;
		if (tweetID != other.tweetID)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TweetComment [tweetID=" + tweetID + ", userName=" + userName
				+ ", screnName=" + screnName + ", lattitude=" + lattitude
				+ ", longitude=" + longitude + ", source=" + source
				+ ", tweetText=" + tweetText + ", tweetTime=" + tweetTime
				+ ", sentimentWordCountMap=" + sentimentWordCountMap + "]";
	}

	

	
	
	

}
