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
import com.digitalpebble.behemoth.DocumentFilter;

/**
 * Converts CommonCrawl data from S3 to BehemothDocuments on the filesystem
 * configured in Hadoop. Uses the DocumentFilter so the parameters
 * "document.filter.url.keep" or "document.filter.mimetype.keep" can be used to
 * limit what is stored in output.
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

    DocumentFilter filter;

    public void map(Text key, ArcFileItem doc,
            OutputCollector<Text, BehemothDocument> collector, Reporter reported)
            throws IOException {
        BehemothDocument newDoc = new BehemothDocument();
        newDoc.setUrl(doc.getUri());
        newDoc.setContent(doc.getContent().getReadOnlyBytes());
        newDoc.setContentType(doc.getMimeType());
        if (filter.keep(newDoc))
            collector.collect(key, newDoc);
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        filter = DocumentFilter.getFilters(job);
    }

}
