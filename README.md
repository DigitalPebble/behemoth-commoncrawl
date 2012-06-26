behemoth-commoncrawl
====================

Standalone CommonCrawl module for Behemoth. 
NOTE : YOU NEED TO HAVE AN AWS ACCOUNT AND SET AWS_ACCESS_KEY AND AWS_SECRET_ACCESS_KEY.

INSTRUCTIONS
- git clone git@github.com:DigitalPebble/behemoth
- mvn package
(note : this won't be required when behemoth-core is publicly available)
- git clone git@github.com:DigitalPebble/behemoth-commoncrawl.git
- mvn install:install-file -DgroupId=org.commoncrawl -DartifactId=commoncrawl -Dversion=0.1-nolibs -Dpackaging=jar -Dfile=lib/commoncrawl-0.1-nolibs.jar
- mvn package
- export HADOOP_USER_CLASSPATH_FIRST=true
- export HADOOP_CLASSPATH=~/.m2/repository/net/java/dev/jets3t/jets3t/0.7.1/jets3t-0.7.1.jar
- hadoop jar ./target/behemoth-commoncrawl-1.0-SNAPSHOT-job.jar  com.digitalpebble.behemoth.commoncrawl.CommonCrawlConverterJob -D mapreduce.user.classpath.first=true -D jets3t.arc.source.aws.access.key.id=$AWS_ACCESS_KEY -D jets3t.arc.source.aws.secret.access.key=$AWS_SECRET_ACCESS_KEY common-crawl/crawl-002/2010/01/06/1/1262850727084 test-crawl
- check the output with 
 - hadoop jar ./target/behemoth-commoncrawl-1.0-SNAPSHOT-job.jar  com.digitalpebble.behemoth.util.CorpusReader -i test-crawl 
