package com.sapient.storm_meetup.localtopology;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.IOUtils;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MeetupCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private Writer writer;
	private TimerTask writerTask;
	private Integer countMessages;
	private final Map<String, Integer> categoryCountMap = new HashMap<String, Integer>();
	private final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"HH:mm:ss.SSS");

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			// Open a handle to the output file
			writer = new FileWriter("stormoutput.csv");
			// Write the header record to the file
			IOUtils.write("TIME,CATEGORY,COUNT", writer);
			IOUtils.write("\n", writer);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

		countMessages = 0;

		// Schedule a timer to write to the file
		Timer timer = new Timer("File writer timer");
		writerTask = new WriterTask();
		timer.schedule(writerTask, 200, 200);
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		/*
		 * if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
		 * && input.getSourceStreamId().equals(
		 * Constants.SYSTEM_TICK_STREAM_ID)) { // Write to the file }
		 */

		countMessages++;
		// Increment count for category
		String category = input.getStringByField("category");
		if (categoryCountMap.containsKey(category)) {
			Integer categoryCount = categoryCountMap.get(category);
			categoryCount++;
			categoryCountMap.put(category, categoryCount);
		} else {
			categoryCountMap.put(category, 1);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	/*
	 * @Override public Map<String, Object> getComponentConfiguration() {
	 * Map<String, Object> conf = new HashMap<String, Object>();
	 * conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3); return conf; }
	 */

	public void cleanup() {
		System.out
				.println("################################# Total number of messages processed by count bolt ::::"
						+ countMessages);
		writerTask.cancel();
		IOUtils.closeQuietly(writer);
	}

	private class WriterTask extends TimerTask implements Serializable {

		private static final long serialVersionUID = 1418697715125105827L;

		@Override
		public void run() {
			Date timeNow = new Date();
			for (String category : categoryCountMap.keySet()) {
				try {
					// Write the category and count to output file
					IOUtils.write(dateFormat.format(timeNow) + "," + category
							+ "," + categoryCountMap.get(category), writer);
					IOUtils.write("\n", writer);
				} catch (IOException e) {
					System.out.println(e.getMessage());
				}
			}
		}

	}

}
