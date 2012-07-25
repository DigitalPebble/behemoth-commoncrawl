package org.commoncrawl.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A input format the reads arc files.
 */
public class ArcInputFormat
  extends FileInputFormat<Text, ArcRecord> {

  /**
   * Returns the <code>RecordReader</code> for reading the arc file.
   * 
   * @param split The InputSplit of the arc file to process.
   * @param job The job configuration.
   * @param reporter The progress reporter.
   */
  public RecordReader<Text, ArcRecord> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    reporter.setStatus(split.toString());
    return new ArcRecordReader(job, (FileSplit)split);
  }
}

