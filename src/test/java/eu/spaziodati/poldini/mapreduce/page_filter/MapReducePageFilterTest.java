package eu.spaziodati.poldini.mapreduce.page_filter;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import eu.spaziodati.poldini.avro.Page;
import eu.spaziodati.poldini.mapreduce.utils.TestUtils;

public class MapReducePageFilterTest {
	String TEST_INPUT_FOLDER = "output_test/filter/";
	String TEST_INPUT_FILE = TEST_INPUT_FOLDER + "test_page_file.avro";
	String TEST_DATAGEM_INPUT_FILE = TEST_INPUT_FOLDER + "test_datagem_file.avro";
	String TEST_OUTPUT_FOLDER = TEST_INPUT_FOLDER + "output_filter_test";

	@Before
	public void setUp() throws Exception {

		// create datagem test file
		File datagem;
		if ((datagem = TestUtils.createFile(TEST_DATAGEM_INPUT_FILE)) != null) {
			Page pageA = new Page("www.a.it", "datagem", "datagem");
			Page pageB = new Page("www.b.it", "datagem", "datagem");
			Page pageC = new Page("www.c.it", "datagem", "datagem");
			TestUtils.marshal(datagem, pageA, pageB, pageC);
		}

		// create snapshot test file
		File snapshot;
		if ((snapshot = TestUtils.createFile(TEST_INPUT_FILE)) != null) {
			Page pageA = new Page("www.a.it", "a", "a");
			Page pageB = new Page("www.b.it", "b", "b");
			Page pageD = new Page("www.d.it", "d", "d");
			TestUtils.marshal(snapshot, pageA, pageB, pageD);
		}
	}

	@Test
	public void testRun() {
		try {
			String[] args = { TEST_INPUT_FOLDER, TEST_OUTPUT_FOLDER };
			int res = -1;
			res = ToolRunner.run(new MapReducePageFilter(), args);

			assert (res == 0);
			File file = new File(TEST_OUTPUT_FOLDER + "/part-r-00000.avro");
			ArrayList<Page> pages = TestUtils.unmarshal(file, Page.class);
			assertTrue(pages.size()>0);
			assertTrue(pages.contains(new Page("www.a.it", "a", "a")));
			assertTrue(pages.contains(new Page("www.b.it", "b", "b")));
			assertFalse(pages.contains(new Page("www.c.it", "c", "c")));
			assertFalse(pages.contains(new Page("www.d.it", "d", "d")));
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
