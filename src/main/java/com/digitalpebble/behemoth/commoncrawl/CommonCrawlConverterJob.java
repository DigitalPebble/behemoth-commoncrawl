package com.digitalpebble.behemoth.commoncrawl;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Converts CommonCrawl data from S3 to BehemothDocuments on the filesystem
 * configured in Hadoop
 **/
public class CommonCrawlConverterJob extends Configured implements Tool {

	/**
	 * Contains the Amazon S3 bucket holding the CommonCrawl corpus.
	 */
	private static final String CC_BUCKET = "aws-publicdatasets";

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			String syntax = "hadoop jar job.jar "
					+ CommonCrawlConverterJob.class.getName()
					+ " inputprefix output";
			System.err.println(syntax);
			return -1;
		}

		String inputPrefixes = args[0];

		Path output = new Path(args[1]);

		// Creates a new job configuration for this Hadoop job.
		JobConf conf = new JobConf(this.getConf());

		// Configures this job with your Amazon AWS credentials
		// conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
		// conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
		conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);
		conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);

		conf.setJobName(getClass().getName());
		conf.setJarByClass(CommonCrawlConverterJob.class);

		// Configures where the input comes from when running our Hadoop job,
		// in this case, gzipped ARC files from the specified Amazon S3 bucket
		// paths.
		ARCInputFormat.setARCSourceClass(conf, JetS3tARCSource.class);
		ARCInputFormat inputFormat = new ARCInputFormat();
		inputFormat.configure(conf);
		conf.setInputFormat(ARCInputFormat.class);

		// Output
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BehemothDocument.class);
		FileOutputFormat.setOutputPath(conf, output);
		conf.setMapperClass(ArcToBehemothTransformer.class);
		conf.setNumReduceTasks(0); // map-only

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CommonCrawlConverterJob(), args);
	}

}

class ArcToBehemothTransformer extends MapReduceBase implements
		Mapper<Text, ArcFileItem, Text, BehemothDocument> {

	public void map(Text key, ArcFileItem doc,
			OutputCollector<Text, BehemothDocument> collector, Reporter reported)
			throws IOException {
		BehemothDocument newDoc = new BehemothDocument();
		newDoc.setUrl(doc.getUri());
		newDoc.setContent(doc.getContent().getReadOnlyBytes());
		newDoc.setContentType(doc.getMimeType());
		collector.collect(key, newDoc);
	}

}
