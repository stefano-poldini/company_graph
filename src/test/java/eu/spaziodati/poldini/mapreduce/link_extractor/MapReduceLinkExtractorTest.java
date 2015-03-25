package eu.spaziodati.poldini.mapreduce.link_extractor;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import eu.spaziodati.poldini.avro.Page;
import eu.spaziodati.poldini.avro.SimpleLink;
import eu.spaziodati.poldini.mapreduce.utils.TestUtils;

public class MapReduceLinkExtractorTest {
	String TEST_INPUT_FOLDER = "output_test/link_extractor";
	String TEST_INPUT_FILE = TEST_INPUT_FOLDER + "test_page_file.avro";
	String TEST_OUTPUT_FOLDER = TEST_INPUT_FOLDER + "output_link_extractor_test";

	@Test
	public void mapReduceLinkExtractorTest() {
		try {
			File file;
			if ((file = TestUtils.createFile(TEST_INPUT_FILE)) != null) {
				Page page1 = TestUtils.createPage("a", "b");
				Page page2 = TestUtils.createPage("b", "c", "d");
				Page page3 = TestUtils.createPage("c", "a", "b");
				marshalTestDataset(file, page1, page2, page3);
			}
			String[] args = { TEST_INPUT_FILE, TEST_OUTPUT_FOLDER };
			int res = -1;
			try {
				res = ToolRunner.run(new MapReduceLinkExtractor(), args);
			} catch (Exception e) {
				e.printStackTrace();
			}
			assert (res == 0);
			file = new File(TEST_OUTPUT_FOLDER + "/part-r-00000.avro");
			ArrayList<SimpleLink> links = TestUtils.unmarshal(file, SimpleLink.class);
			assertTrue (links.contains(new SimpleLink("http://www.a.it", "http://www.b.it")));
			assertTrue (links.contains(new SimpleLink("http://www.b.it", "http://www.c.it")));
			assertTrue (links.contains(new SimpleLink("http://www.c.it", "http://www.a.it")));
			assertTrue (links.contains(new SimpleLink("http://www.c.it", "http://www.b.it")));
			assertFalse (links.contains(new SimpleLink("http://www.b.it", "http://www.d.it")));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void marshalTestDataset(File file, Page... pages) throws IOException {
		TestUtils.marshal(file, pages);
	}

}
