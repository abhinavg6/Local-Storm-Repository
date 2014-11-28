package com.sapient.storm_meetup.localtopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MeetupStormTopology {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		// create an instance of TopologyBuilder class
		TopologyBuilder builder = new TopologyBuilder();
		// set the group spout class
		builder.setSpout("eventspout", new MeetupEventSpout(), 1);
		// set the matcher bolt class
		builder.setBolt("matcherbolt", new MeetupMatcherBolt(), 2)
				.fieldsGrouping("eventspout", new Fields("groupName"));
		// set the count bolt class
		builder.setBolt("countbolt", new MeetupCountBolt(), 1).globalGrouping(
				"matcherbolt");

		Config conf = new Config();
		conf.setDebug(true);
		// create an instance of LocalCluster class for
		// executing topology in local mode.
		LocalCluster cluster = new LocalCluster();

		// MeetupStormTopology is the name of submitted topology.
		cluster.submitTopology("MeetupStormTopology", conf,
				builder.createTopology());
		try {
			Thread.sleep(13500);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		// kill the MeetupStormTopology
		cluster.killTopology("MeetupStormTopology");
		// shutdown the storm test cluster
		cluster.shutdown();
	}

}
