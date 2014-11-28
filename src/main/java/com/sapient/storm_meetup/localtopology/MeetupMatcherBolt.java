package com.sapient.storm_meetup.localtopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A bolt to map each incoming event tuple to a category, and emits the event
 * category as a tuple
 * 
 * @author abhinavg6
 *
 */
public class MeetupMatcherBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	// Map used for categorization of events
	private static final Map<String, String> categoryMap = new HashMap<String, String>();
	static {
		categoryMap.put("NoSQL", "NoSQL/Big Data");
		categoryMap.put("MongoDB", "NoSQL/Big Data");
		categoryMap.put("Neo4J", "NoSQL/Big Data");
		categoryMap.put("Cassandra", "NoSQL/Big Data");
		categoryMap.put("CouchDB", "NoSQL/Big Data");
		categoryMap.put("Storm", "NoSQL/Big Data");
		categoryMap.put("Spark", "NoSQL/Big Data");
		categoryMap.put("Spring", "Spring");
		categoryMap.put("Web", "Web Programming");
		categoryMap.put("HTML5", "Web Programming");
		categoryMap.put("websocket", "Web Programming");
		categoryMap.put("Javascript", "Web Programming");
		categoryMap.put("Node.js", "Web Programming");
		categoryMap.put("REST", "Web Programming");
		categoryMap.put("jQuery", "Web Programming");
		categoryMap.put("ExtJS", "Web Programming");
		categoryMap.put("Wordpress", "Web Programming");
		categoryMap.put("Grails", "Web Programming");
		categoryMap.put("Scala", "Scala");
		categoryMap.put("Python", "Python");
		categoryMap.put(".NET", "Microsoft tech");
		categoryMap.put("Windows", "Microsoft tech");
		categoryMap.put("Mobile", "Mobile");
		categoryMap.put("Android", "Mobile");
		categoryMap.put("iOS", "Mobile");
		categoryMap.put("NFC", "Mobile");
		categoryMap.put("AWS", "Cloud");
		categoryMap.put("Amazon Web", "Cloud");
		categoryMap.put("Azure", "Cloud");
		categoryMap.put("Google App Engine", "Cloud");
	}

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		// fetched the field "eventName" from input tuple.
		String eventName = input.getStringByField("eventName");
		List<String> emittableList = new ArrayList<String>();

		// Map an event to different categories - Not very efficient as it uses
		// a simple map for classification
		for (String keyword : categoryMap.keySet()) {
			boolean isMatched = Pattern
					.compile(Pattern.quote(keyword), Pattern.CASE_INSENSITIVE)
					.matcher(eventName).find();
			if (isMatched) {
				String category = categoryMap.get(keyword);
				if (!emittableList.contains(category)) {
					emittableList.add(category);
				}
			}
		}

		// Emit one or more tuples for different mapped categories, anchored by
		// input tuple
		for (String categoryToEmit : emittableList) {
			collector.emit(input, new Values(eventName, categoryToEmit));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// emit the tuple with field "eventName" and "category"
		declarer.declare(new Fields("eventName", "category"));
	}

}
