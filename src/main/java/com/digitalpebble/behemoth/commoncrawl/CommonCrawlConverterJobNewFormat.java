package com.digitalpebble.behemoth.commoncrawl;

// Java classes
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.hadoop.mapred.ArcInputFormatCC;
import org.commoncrawl.hadoop.mapred.ArcRecordCC;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentFilter;

public class CommonCrawlConverterJobNewFormat extends Configured implements
        Tool {

    private static final Logger LOG = Logger
            .getLogger(CommonCrawlConverterJobNewFormat.class);

    public static class ConversionMapper extends MapReduceBase implements
            Mapper<Text, ArcRecordCC, Text, BehemothDocument> {

        DocumentFilter filter;

        // used to ensure that we don't get more than 120 Counters
        Set<String> mimetypeset = new HashSet<String>();

        public void map(Text key, ArcRecordCC doc,
                OutputCollector<Text, BehemothDocument> collector,
                Reporter reported) throws IOException {
            BehemothDocument newDoc = new BehemothDocument();
            newDoc.setUrl(doc.getURL());
            int startContent = _searchForCRLFCRLF(doc.getPayload());
            if (startContent==-1) {
                startContent = 0;
            }
            
            // could also do 
            // doc.getHttpResponse().getEntity().getContent();
            
            byte[] content = new byte[doc.getPayload().length-startContent];
            System.arraycopy(doc.getPayload(), startContent, content, 0, content.length);

            newDoc.setContent(content);
            newDoc.setContentType(doc.getContentType());
            // TODO set IP address, HTTP headers etc...
            if (filter.keep(newDoc)) {
                collector.collect(key, newDoc);
                reported.incrCounter("COMMON CRAWL", "KEPT", 1L);
            } else
                reported.incrCounter("COMMON CRAWL", "FILTERED", 1L);
        }

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            filter = DocumentFilter.getFilters(job);
        }

        private int _searchForCRLFCRLF(byte[] data) {

            final byte CR = (byte) '\r';
            final byte LF = (byte) '\n';

            int i;
            int s = 0;

            for (i = 0; i < data.length; i++) {

                if (data[i] == CR) {
                    if (s == 0)
                        s = 1;
                    else if (s == 1)
                        s = 0;
                    else if (s == 2)
                        s = 3;
                    else if (s == 3)
                        s = 0;
                } else if (data[i] == LF) {
                    if (s == 0)
                        s = 0;
                    else if (s == 1)
                        s = 2;
                    else if (s == 2)
                        s = 0;
                    else if (s == 3)
                        s = 4;
                } else {
                    s = 0;
                }

                if (s == 4)
                    return i + 1;
            }

            return -1;
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
            // add the text as content
            newDoc.setContent(doc.getBytes());
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
            job.setInputFormat(ArcInputFormatCC.class);
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
