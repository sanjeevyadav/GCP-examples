/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.analyticqa.postgresdatamigration;

import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.sun.tools.sjavac.Log;


/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipelinepoc {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipelinepoc.class);
  
  private static DataflowPipelineOptions options;


  public static void main(String[] args) throws FileNotFoundException, IOException, PropertyVetoException, BatchUpdateException {
	    options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
	    options.setAppName("TestAppName");
	    options.setProject("project-name");
	    options.setTempLocation("gs://dataflow_test/temp");// 
	    options.setStagingLocation("gs://dataflow_test/stage");
	    options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
	    options.setNumWorkers(5);
	    options.setMaxNumWorkers(10);
	    options.setWorkerMachineType("n1-highmem-4");
	    options.setDiskSizeGb(500);
	    GoogleCredentials credentials = ServiceAccountCredentials. fromStream(new FileInputStream("credential.json"))
		        .createScoped(Arrays.asList(new String[] { "https://www.googleapis.com/auth/cloud-platform" }));
	    options.setGcpCredential(credentials);
	    options.setRunner(DataflowRunner.class);
	     
	    Pipeline p = Pipeline.create(options);
		      
            PCollection<KV<String, String>> sqlResult = p.apply(JdbcIO.<KV<String, String>>read()
	    		  .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://ip:5432/books") 
	    		  .withUsername("username")
	    		   .withPassword("password"))
	    		  
	              .withQuery("select * from author").withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
	              
	              .withRowMapper(new JdbcIO.RowMapper<KV<String, String>>() {

	                  private static final long serialVersionUID = 1L;
	                  @Override
	                  public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
	                	  ResultSetMetaData md = resultSet.getMetaData();
	                	  return KV.of(resultSet.getString(1),resultSet.getString(2)  + "," + resultSet.getString(3) + "," + resultSet.getString(4)
								+ "," + resultSet.getString(5)+ "," + resultSet.getString(6)+ "," + resultSet.getString(7)
								+ "," + resultSet.getString(8)+ "," + resultSet.getString(9)+ "," + resultSet.getString(10)
								+ "," + resultSet.getString(11)+ "," + resultSet.getString(12)+ "," + resultSet.getString(13)
								+ "," + resultSet.getString(14)+ "," + resultSet.getString(15));
	                  }
	              }));
	      
	      

	   sqlResult.apply(JdbcIO.<KV<String, String>>write()
	    		  .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://ip:5432/books")
	    		  .withUsername("username")
	    		  .withPassword("password"))
	    		  .withStatement("insert into author values(?, ?, ?,?,?,?,?,?,?,?,?,?,?)")
	    		  .withPreparedStatementSetter((element, query) -> {
	    			  
                     query.setInt(1, Integer.parseInt(element.getKey()));
                     String strArr[] =  element.getValue().split(",");
                      query.setString(2, strArr[0]);
                      query.setString(3,strArr[1]);
                      query.setString(4,strArr[4]);
                      query.setString(5,strArr[5]);
                      query.setString(6,strArr[6]);
                      query.setString(7,strArr[7]);
                      query.setString(8,strArr[8]);
                      query.setString(9,strArr[9]);
                      query.setString(10,strArr[10]);
                      query.setString(11,strArr[11]);
                      query.setString(12,strArr[12]);
                      query.setString(13,strArr[13]);
               
                      
                  })
	    		  );
    p.run();
  }
}
