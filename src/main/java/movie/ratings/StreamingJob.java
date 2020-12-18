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

package movie.ratings;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {


	private static final String DELIMITER = "\n";


	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		  boolean isRunning = true;



		DataStream<Ysb.YSBRecord> file = env.addSource(new Ysb.YSBSource());

		DataStream<Ysb.YSBRecord> filtered = file.filter(value -> value.eventType.equals("view"));

		DataStream<String> concat = filtered.map(new MapFunction<Ysb.YSBRecord, String>() {

			@Override
			public String map(Ysb.YSBRecord line) throws Exception {
				return line.getEventType();
			}

		});

		concat.print();



	/*	Socket echoSocket = new Socket("localhost", 31000);
		PrintWriter out =
				new PrintWriter(echoSocket.getOutputStream(), true);
		BufferedReader input  = new BufferedReader(
				new InputStreamReader(echoSocket.getInputStream(), StandardCharsets.UTF_8));

		String line;
		StringBuilder sb = new StringBuilder();
		out.println( 0 + ":persons");
		line = input.readLine();
		while ((line = input.readLine()) != null) {
			System.out.println(line);
		}
*/


/*		DataStream<String> concat = file.map(new MapFunction<String, String>() {
			@Override
			public String map(String line) {
				return line.concat("," + System.nanoTime());
			}
		});


		DataStream<MovieRating> cal = concat.map(new MapFunction<String, MovieRating>() {
			@Override
			public MovieRating map(String line) {
				String [] splitLine = line.split(",");
				MovieRating movieRating = new MovieRating(splitLine[0], splitLine[1] ,splitLine[2] ,splitLine[3],splitLine[4], System.nanoTime()+"");
				//return line.replaceAll(splitLine[4], System.nanoTime() - Long.parseLong(splitLine[4]) + "");
				return movieRating;
			}
		});
		cal.print();*/
		/*cal.writeAsCsv("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/java/movie/ratings/calculateRatings1m.csv").setParallelism(1);
		System.out.println("Throughput: " + (System.nanoTime() - startTime));*/
		env.execute("Flink Streaming Java API Skeleton");
		//System.out.println("Throughput: " + (System.nanoTime() - startTime));

	}

}
