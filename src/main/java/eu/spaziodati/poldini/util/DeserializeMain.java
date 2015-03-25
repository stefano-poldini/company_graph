package eu.spaziodati.poldini.util;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import eu.spaziodati.poldini.avro.Page;

public class DeserializeMain {
	private static final String FILE_PATH = "/home/stich/Documents/university/master thesis/Spazio dati/project/avro/avro test file/part-m-02294.avro";

	// private static final String FILE_PATH =
	// "/home/stich/workspace/company_graph/output/part-r-00000.avro";
	// private static final String FILE_PATH =
	// "/home/stich/workspace/company_graph/src/main/avro/datagem pages.avro";

	public static void main(String[] args) throws IOException {
		File file = new File(FILE_PATH);
		DatumReader<Page> pageDatumReader = new SpecificDatumReader<Page>(Page.class);
		DataFileReader<Page> pageDataFileReader = new DataFileReader<Page>(file, pageDatumReader);
		Page page = null;
		for (int i = 0; i < 100; i++) {
			page = pageDataFileReader.next();
			System.out.println(page.getUrl());
		}
		pageDataFileReader.close();
	}
}
