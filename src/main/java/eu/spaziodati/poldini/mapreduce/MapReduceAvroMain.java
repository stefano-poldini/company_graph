package eu.spaziodati.poldini.mapreduce;

import org.apache.hadoop.util.ToolRunner;

import eu.spaziodati.poldini.mapreduce.graph.creator.MapReduceGraphCreator;
import eu.spaziodati.poldini.mapreduce.link_extractor.MapReduceLinkExtractor;
import eu.spaziodati.poldini.mapreduce.page_filter.MapReducePageFilter;
import eu.spaziodati.poldini.utils.Configuration;
import eu.spaziodati.poldini.utils.DatagemDownload;

public class MapReduceAvroMain {

	public static void main(String[] args)  {

		if (args.length != 2) {
			System.err.println("Usage: MapReduceAvroMain <s3_output_directory> <s3_avro_data_path>");
			for (int i = 0; i < args.length; i++) {
				System.err.println(args[i]);
			}
			if (args.length==0){
				System.err.println("lista argomenti vuota");
			}
			return;
		}

		String outputDir=args[0];
		String data=args[1];
		Configuration conf=new Configuration(outputDir);
		
		
		try {
			DatagemDownload.download();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int res=-1;
		final String[] filter_args = { conf.datagem_avro_output_file, data,
				conf.map_reduce_filter_output };
		
		try {
			res = ToolRunner.run(new MapReducePageFilter(), filter_args);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (res != 0) {
			return;
		}

		final String[] link_extractor_args = { conf.map_reduce_link_extractor_input,
				conf.map_reduce_link_extractor_output };
		try {
			res = ToolRunner.run(new MapReduceLinkExtractor(), link_extractor_args);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (res != 0) {
			return;
		}

		final String[] graph_creator_args = { conf.map_reduce_graph_creator_input,
				conf.map_reduce_graph_creator_output };
		try {
			res = ToolRunner.run(new MapReduceGraphCreator(), graph_creator_args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
