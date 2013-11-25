/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.behemoth.commoncrawl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.BehemothMapper;
import com.digitalpebble.behemoth.DocumentFilter;

/**
 * Utility class used to merge the content of Behemoth SequenceFiles.
 **/
public class CorpusMerger extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory
			.getLogger(CorpusMerger.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(BehemothConfiguration.create(),
				new CorpusMerger(), args);
		System.exit(res);
	}

	public class MergerReducer implements
			Reducer<Text, BehemothDocument, Text, BehemothDocument> {

		private DocumentFilter docFilter;

		public void configure(JobConf conf) {
			this.docFilter = DocumentFilter.getFilters(conf);
		}

		public void close() throws IOException {
		}

		public void reduce(Text key, Iterator<BehemothDocument> iter,
				OutputCollector<Text, BehemothDocument> collector,
				Reporter reporter) throws IOException {

			BehemothDocument doc = null;

			while (iter.hasNext()) {
				BehemothDocument bd = iter.next();
				if (doc == null) {
					doc = bd;
					continue;
				}

				boolean hasMerged = false;

				String newText = bd.getText();
				byte[] newContent = bd.getContent();
				MapWritable newMD = bd.getMetadata();

				if (StringUtils.isNotBlank(newText)
						&& StringUtils.isBlank(doc.getText())) {
					doc.setText(newText);
					hasMerged = true;
				}
				if (doc.getContent() == null && newContent != null) {
					doc.setContent(newContent);
					hasMerged = true;
				}
				if (doc.getMetadata() == null && newMD != null) {
					doc.setMetadata(newMD);
					hasMerged = true;
				}

				if (hasMerged)
					reporter.incrCounter("CorpusMerger", "merged", 1);
				else
					reporter.incrCounter("CorpusMerger", "not_merged", 1);
			}

			if (doc != null) {
				boolean keep = docFilter.keep(doc);
				if (!keep) {
					reporter.incrCounter("CorpusMerger",
							"DOC SKIPPED BY FILTERS", 1);
					return;
				}
				collector.collect(key, doc);
			}
		}
	}

	public int run(String[] args) throws Exception {

		Options options = new Options();
		// automatically generate the help statement
		HelpFormatter formatter = new HelpFormatter();
		// create the parser
		CommandLineParser parser = new GnuParser();

		options.addOption("h", "help", false, "print this message");
		options.addOption("i", "input", true, "input Behemoth corpus");
		options.addOption("o", "output", true, "output Behemoth corpus");

		// parse the command line arguments
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
			String input = line.getOptionValue("i");
			if (line.hasOption("help")) {
				formatter.printHelp("CorpusMerger", options);
				return 0;
			}
			if (input == null) {
				formatter.printHelp("CorpusMerger", options);
				return -1;
			}
		} catch (ParseException e) {
			formatter.printHelp("CorpusMerger", options);
			return -1;
		}

		Path outputPath = new Path(line.getOptionValue("o"));

		String[] paths = (line.getOptionValues("i"));

		JobConf job = new JobConf(getConf());
		// MUST not forget the line below
		job.setJarByClass(this.getClass());

		job.setJobName("CorpusMerger");

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(BehemothDocument.class);
		job.setMapperClass(BehemothMapper.class);

		job.setReducerClass(MergerReducer.class);

		for (String in : paths)
			FileInputFormat.addInputPath(job, new Path(in));

		FileOutputFormat.setOutputPath(job, outputPath);

		try {
			long start = System.currentTimeMillis();
			JobClient.runJob(job);
			long finish = System.currentTimeMillis();
			if (LOG.isInfoEnabled()) {
				LOG.info("CorpusMerger completed. Timing: " + (finish - start)
						+ " ms");
			}
		} catch (Exception e) {
			LOG.error("Exception caught", e);
			// fs.delete(outputPath, true);
		} finally {
		}

		return 0;
	}
}
