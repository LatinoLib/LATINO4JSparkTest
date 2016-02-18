Notes
-----

After importing this project into IDEA (`File > New > Project from Existing Sources`), you need to do the following to make things work:

1. Go to `File > Project Structure`, select `Libraries > + > Java`, add the Spark JAR (e.g., `C:\Spark\lib\spark-assembly-1.6.0-hadoop2.6.0.jar`) and the LATINO4J JAR (e.g., `C:\Work\Java\LATINO4J\latino4j-core\target\latino4j-0.2-SNAPSHOT-jar-with-dependencies.jar`). 
1. Go to `File > Project Structure > Artifacts > + > JAR > From modules with dependencies`. Set `Main Class` to `Latino4SparkTest`. Also check `Build on make`.