behemoth-commoncrawl
====================

Standalone CommonCrawl module for Behemoth

INSTRUCTIONS
- git clone git@github.com:jnioche/behemoth
- mvn package
(note : this won't be required when behemoth-core is publicly available)
- git clone git@github.com:jnioche/behemoth-commoncrawl.git
- mvn install:install-file -DgroupId=org.commoncrawl -DartifactId=commoncrawl -Dversion=0.1-nolibs -Dpackaging=jar -Dfile=lib/commoncrawl-0.1-nolibs.jar
- mvn package
- hadoop jar /data/behemoth-commoncrawl/target/behemoth-commoncrawl-1.0-SNAPSHOT-job.jar  com.digitalpebble.behemoth.commoncrawl.CommonCrawlConverterJob input output

