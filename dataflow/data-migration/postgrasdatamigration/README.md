This repo uses JDBCIO api to connect cloud-sql using java for Dataflow.

       <dependency>
        <groupId>org.apache.beam</groupId>
        <artifactId>beam-sdks-java-io-jdbc</artifactId>
        <version>2.3.0</version>
       </dependency>

       <dependency>
         <groupId>org.postgresql</groupId>
         <artifactId>postgresql</artifactId>
         <version>9.4.1208.jre6</version>
        </dependency>

       <dependency>
         <groupId>com.google.cloud.sql</groupId>
         <artifactId>postgres-socket-factory</artifactId>
         <version>1.2.0</version>
       </dependency>
       
Create Service account to connect with cloud-sql.
