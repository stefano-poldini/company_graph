package eu.spaziodati.poldini.utils;

public class Configuration {

public static final String MAP_REDUCE_OUTPUT="output";
public static final String DATAGEM_AVRO_OUTPUT_FILE = MAP_REDUCE_OUTPUT+"/datagem/datagem pages.avro";
public static final String MAP_REDUCE_FILTER_INPUT_DATAGEM=DATAGEM_AVRO_OUTPUT_FILE;
public static final String MAP_REDUCE_FILTER_INPUT_CRAWLER="./src/main/avro";
public static final String MAP_REDUCE_FILTER_OUTPUT=MAP_REDUCE_OUTPUT+"/filter";
public static final String MAP_REDUCE_LINK_EXTRACTOR_INPUT=MAP_REDUCE_FILTER_OUTPUT;
public static final String MAP_REDUCE_LINK_EXTRACTOR_OUTPUT=MAP_REDUCE_OUTPUT+"/link_extractor";
public static final String MAP_REDUCE_GRAPH_CREATOR_INPUT=MAP_REDUCE_LINK_EXTRACTOR_OUTPUT;
public static final String MAP_REDUCE_GRAPH_CREATOR_OUTPUT=MAP_REDUCE_OUTPUT+"/graph_creator";
}
