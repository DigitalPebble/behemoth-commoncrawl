package com.digitalpebble.behemoth.commoncrawl;

// Java classes
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.commoncrawl.hadoop.mapred.ArcRecord;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentFilter;

public class CommonCrawlConverterJobNewFormat extends Configured implements
        Tool {

    private static final Logger LOG = Logger
            .getLogger(CommonCrawlConverterJobNewFormat.class);

    public static class ConversionMapper extends MapReduceBase implements
            Mapper<Text, ArcRecord, Text, BehemothDocument> {

        DocumentFilter filter;

        public void map(Text key, ArcRecord doc,
                OutputCollector<Text, BehemothDocument> collector,
                Reporter reported) throws IOException {
            BehemothDocument newDoc = new BehemothDocument();
            newDoc.setUrl(doc.getURL());
            newDoc.setContent(doc.getContent());
            newDoc.setContentType(doc.getContentType());
            if (filter.keep(newDoc)) {
                collector.collect(key, newDoc);
                reported.incrCounter("COMMON CRAWL MIMETYPE",
                        doc.getContentType(), 1l);
            } else
                reported.incrCounter("COMMON CRAWL", "FILTERED", 1l);
        }

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            filter = DocumentFilter.getFilters(job);
        }

    }

    public static class TextConversionMapper extends MapReduceBase implements
            Mapper<Text, Text, Text, BehemothDocument> {

        DocumentFilter filter;

        public void map(Text key, Text doc,
                OutputCollector<Text, BehemothDocument> collector,
                Reporter reported) throws IOException {
            BehemothDocument newDoc = new BehemothDocument();
            newDoc.setUrl(key.toString());
            newDoc.setText(doc.toString());
            if (filter.keep(newDoc)) {
                collector.collect(key, newDoc);
            } else
                reported.incrCounter("COMMON CRAWL", "FILTERED", 1l);
        }

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            filter = DocumentFilter.getFilters(job);
        }

    }

    public static class ARCFilter implements PathFilter {

        public boolean accept(Path path) {

            if (!path.getName().endsWith(".arc.gz"))
                return false;
            return true;
        }
    }

    public static class TextFilter implements PathFilter {

        public boolean accept(Path path) {

            if (!path.getName().startsWith("textData-"))
                return false;
            return true;
        }
    }

    /**
     * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
     * 
     * @param args
     *            command line parameters, less common Hadoop job parameters
     *            stripped out and interpreted by the Tool class.
     * @return 0 if the Hadoop job completes successfully, 1 if not.
     */
    public int run(String[] args) throws Exception {

        String outputPath = null;
        String inputPath = null;

        // Read the command line arguments.
        if (args.length < 1)
            throw new IllegalArgumentException(
                    "Example JAR must be passed an input + output path.");

        // String inputPath =
        // "s3n://aws-publicdatasets/common-crawl/parse-output/segment/1341690164240/1341817173109_4.arc.gz";
        // "s3n://aws-publicdatasets/common-crawl/parse-output/segment/*/*.arc.gz";
        inputPath = args[0];
        outputPath = args[1];
        boolean binary = true;
        if (args.length > 2) {
            if ("-text".equalsIgnoreCase(args[2]))
                binary = false;
        }

        // Creates a new job configuration for this Hadoop job.
        JobConf job = new JobConf(this.getConf());

        job.setJarByClass(this.getClass());

        // Scan the provided input path for ARC files.
        LOG.info("setting input path to '" + inputPath + "'");
        FileInputFormat.addInputPath(job, new Path(inputPath));

        if (binary) {
            FileInputFormat.setInputPathFilter(job, ARCFilter.class);
            job.setInputFormat(ArcInputFormat.class);      
            job.setMapperClass(ConversionMapper.class);

        } else {
            FileInputFormat.setInputPathFilter(job, TextFilter.class);
            job.setInputFormat(SequenceFileInputFormat.class);
            job.setMapperClass(TextConversionMapper.class);
        }
        // Delete the output path directory if it already exists.
        LOG.info("clearing the output path at '" + outputPath + "'");

        FileSystem fs = FileSystem.get(new URI(outputPath), job);

        if (fs.exists(new Path(outputPath)))
            fs.delete(new Path(outputPath), true);

        // Set the path where final output 'part' files will be saved.
        LOG.info("setting output path to '" + outputPath + "'");
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Set which OutputFormat class to use.
        job.setOutputFormat(SequenceFileOutputFormat.class);

        // Set the output data types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BehemothDocument.class);

        job.setNumReduceTasks(0); // map-only

        if (JobClient.runJob(job).isSuccessful())
            return 0;
        else
            return 1;
    }

    /**
     * Main entry point that uses the {@link ToolRunner} class to run the
     * example Hadoop job.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CommonCrawlConverterJobNewFormat(), args);
        System.exit(res);
    }
}
