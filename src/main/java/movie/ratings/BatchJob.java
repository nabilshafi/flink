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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment

		long startTime = System.nanoTime();

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<String> file = env.readTextFile("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/java/movie/ratings/ratings.csv");

		file.flatMap(new ExtractRating()) .groupBy(0) .reduceGroup(new SumRatingCount()) .print();
		long stopTime = System.nanoTime();
		double duration = (stopTime - startTime) / 1e9d;
		System.out.println("latency = " + duration);
		System.out.println("Throughput = " + 100000/duration);



		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * https://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */

		// execute program
	}


	private static class ExtractRating implements FlatMapFunction<String, Tuple2<IntValue, Integer>> {
		// Mutable int field to reuse to reduce GC pressure
		IntValue ratingValue = new IntValue();
		// Reuse rating value and result tuple
		Tuple2<IntValue, Integer> result = new Tuple2<>(ratingValue, 1);
		@Override public void flatMap(String s, Collector<Tuple2<IntValue, Integer>> collector) throws Exception {
			// Every line contains comma separated values // user id | item id | rating | timestamp
			String[] split = s.split(",");
			String ratingStr = split[2];
			// Ignore CSV header
			if (!ratingStr.equals("rating")) {
				int rating = (int) Double.parseDouble(split[2]);
				ratingValue.setValue(rating);
				collector.collect(result);
			}
		}
	}
	private static class SumRatingCount implements GroupReduceFunction<Tuple2<IntValue, Integer>, Tuple2<IntValue, Integer>> {
		@Override public void reduce(Iterable<Tuple2<IntValue, Integer>> iterable, Collector<Tuple2<IntValue, Integer>> collector) throws Exception {
			IntValue rating = null;
			int ratingsCount = 0;
			for (Tuple2<IntValue, Integer> tuple : iterable) {
				rating = tuple.f0;
				ratingsCount += tuple.f1;
			}
			collector.collect(new Tuple2<>(rating, ratingsCount));
		}
	}


}
