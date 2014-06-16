package io.comress;

import org.apache.cassandra.io.compress.CompressionMetadata;

public class CompressionMetadataTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CompressionMetadata metadata=CompressionMetadata.create("E:\\tmp\\var\\lib\\cassandra\\data\\system\\schema_columns\\system-schema_columns-ic-7-CompressionInfo.db");
		System.out.println(metadata);
	}

}
