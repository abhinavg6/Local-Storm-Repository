package com.sapient.storm_meetup.localtopology;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A spout reading an meetup event input file and emitting event tuples
 * 
 * @author abhinavg6
 *
 */
public class MeetupEventSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector spoutOutputCollector;
	private Iterable<CSVRecord> csvRecords;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// Open the spout
		this.spoutOutputCollector = collector;
		// Read the input csv file
		try {
			InputStream inStream = this.getClass().getClassLoader()
					.getResourceAsStream("meetupoutput.csv");
			Reader in = new InputStreamReader(inStream);
			csvRecords = CSVFormat.DEFAULT
					.withHeader("GROUP_NAME", "EVENT_NAME", "EVENT_STATUS",
							"EVENT_CITY", "EVENT_COUNTRY").parse(in);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void nextTuple() {
		// Storm cluster repeatedly calls this method to emit a continuous
		// stream of tuples.
		if (csvRecords.iterator().hasNext()) {
			// Get the next record from input file
			CSVRecord csvRecord = null;
			try {
				csvRecord = csvRecords.iterator().next();
				String groupName = csvRecord.get("GROUP_NAME");
				String eventName = csvRecord.get("EVENT_NAME");
				String eventStatus = csvRecord.get("EVENT_STATUS");
				String eventCity = csvRecord.get("EVENT_CITY");
				String eventCountry = csvRecord.get("EVENT_COUNTRY");

				// Emit the record as a tuple
				spoutOutputCollector.emit(new Values(groupName, eventName,
						eventStatus, eventCity, eventCountry));
			} catch (NoSuchElementException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// emit the tuple with all fields - not all are used down the line
		// though
		declarer.declare(new Fields("groupName", "eventName", "eventStatus",
				"eventCity", "eventCountry"));
	}

}
