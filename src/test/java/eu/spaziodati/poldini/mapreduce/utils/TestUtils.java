package eu.spaziodati.poldini.mapreduce.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import eu.spaziodati.poldini.avro.Page;
import eu.spaziodati.poldini.utils.Utils;

public class TestUtils {
	public static <T extends SpecificRecordBase> ArrayList<T> unmarshal(File file, Class<T> avroClass)
			throws IOException {
		DatumReader<T> tDatumReader = new SpecificDatumReader<T>(avroClass);
		DataFileReader<T> tDataFileReader = new DataFileReader<T>(file, tDatumReader);
		T t = null;
		ArrayList<T> list = new ArrayList<T>();
		while (tDataFileReader.hasNext()) {
			t = tDataFileReader.next();
			System.out.println(t);
			list.add(t);
		}
		tDataFileReader.close();
		return list;
	}

	public static <T extends SpecificRecordBase> void marshal(File file, T... objects) throws IOException {

		DatumWriter<T> datumWriter = new SpecificDatumWriter<T>();
		DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(datumWriter).create(objects[0].getSchema(), file);
		for (T obj : objects) {
			dataFileWriter.append(obj);
		}
		dataFileWriter.close();
	}

	// creates a page where the content contains hyperlinks to the targets pages
	public static Page createPage(String source, String... targets) {
		char quotes = '"';
		String content = "<html><head>" + "</head><body>";
		for (String target : targets) {
			content += "<a href=" + quotes + "www." + target + ".it" + quotes + ">reference to " + target + "</a>";
		}
		;
		content += "</body></html>";
		System.out.println(content);
		Page page = new Page("www." + source + ".it", Matcher.quoteReplacement(content), "");
		return page;
	};

	/**
	 * creates a new empty file
	 * @param path the path of the file to create
	 * @return the new file if path does not exist and null otherwise
	 * @throws IOException
	 */
	public static File createFile(String path) throws IOException {
		return Utils.createFile(path);
	}
}
