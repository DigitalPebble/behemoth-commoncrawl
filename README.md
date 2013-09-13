behemoth-commoncrawl
====================

CommonCrawl module for Behemoth. 
NOTE : YOU NEED TO HAVE AN AWS ACCOUNT AND SET AWS_ACCESS_KEY AND AWS_SECRET_ACCESS_KEY.

INSTRUCTIONS
- git clone git@github.com:DigitalPebble/behemoth-commoncrawl.git
- mvn install:install-file -DgroupId=org.commoncrawl -DartifactId=commoncrawl -Dversion=1.0 -Dpackaging=jar -Dfile=lib/commoncrawl-1.0.jar
- mvn clean install
- hadoop jar ./target/behemoth-commoncrawl-1.1-SNAPSHOT-job.jar com.digitalpebble.behemoth.commoncrawl.CommonCrawlConverterJob2012 -D fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY -D fs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY s3n://aws-publicdatasets/common-crawl/parse-output/segment/1350433107105/* cc-test
- check the output with 
 - hadoop jar ./target/behemoth-commoncrawl-1.1-SNAPSHOT-job.jar com.digitalpebble.behemoth.util.CorpusReader -i cc-test 
 
The converter can also take as input the text version of the cc dataset by adding the -text parameter e.g.

hadoop jar ./target/behemoth-commoncrawl-1.1-SNAPSHOT-job.jar com.digitalpebble.behemoth.commoncrawl.CommonCrawlConverterJob2012 -D fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY -D fs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY s3n://aws-publicdatasets/common-crawl/parse-output/segment/1350433107105/* cc-test -text

which adds the text found to the text field of the BehemothDocuments. Note that the fields contentType and content are not set as they are pertaining to the original content and not the extracted text.







