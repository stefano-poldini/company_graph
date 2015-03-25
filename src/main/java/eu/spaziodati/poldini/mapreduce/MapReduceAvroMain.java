package eu.spaziodati.poldini.mapreduce;

import org.apache.hadoop.util.ToolRunner;

import eu.spaziodati.poldini.mapreduce.link_extractor.MapReduceLinkExtractor;
import eu.spaziodati.poldini.mapreduce.page_filter.MapReducePageFilter;

public class MapReduceAvroMain {
	private static final String FILE_PATH = "/home/stich/Documents/university/master thesis/Spazio dati/project/avro/avro test file/part-m-02294.avro";

	public static void main(String[] args) throws Exception {
//		args[0]=FILE_PATH;
//		args[1]="output";
		ToolRunner.run(new MapReduceLinkExtractor(), args);
	}

}
