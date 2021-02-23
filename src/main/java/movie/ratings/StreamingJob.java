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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import movie.ratings.BidEventTimestampExtractor;
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

		long startTime = System.currentTimeMillis();

		DataStream<Ysb.BidEvent> file = env.addSource(new Ysb.YSBSource()).setParallelism(1);
		//DataStream<String> text = env.readTextFile("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/java/movie/ratings/ratings100k.csv").setParallelism(1);
		/*DataStream<Ysb.MovieRating> file = text
				.map((line) -> {
					line += "," + System.currentTimeMillis();
					String[] str = line.split(",");
					Ysb.MovieRating event = new Ysb.MovieRating(Long.parseLong(str[3]), Long.parseLong(str[0]),Long.parseLong(str[1]),
							 Double.parseDouble(str[2]), Long.parseLong(str[4]));
					return event;
				});
*/
		/*DataStream<Tuple4<Long, Long, Double, Long>> concat = text.map(new MapFunction<String, Tuple4<Long, Long, Double, Long>>() {

			@Override
			public Tuple4<Long, Long, Double, Long> map(String s) throws Exception {
				String[] str = s.split(",");
				return Tuple4.of(Long.parseLong(str[0]),Long.parseLong(str[1]),Double.parseDouble(str[2]),Long.parseLong(str[4]));
			}


		}).setParallelism(1);*/



		//DataStream<Tuple4<Long, Long, Double, Long>> concat3 =  concat.keyBy(0).timeWindow(Time.milliseconds(200),Time.milliseconds(100)).max(3).setParallelism(1);


//MaxBid




		DataStream<Tuple6<Long,Long, Integer,Long, Long,Long>>  county = file.assignTimestampsAndWatermarks(new BidEventTimestampExtractor()).
				keyBy((Ysb.BidEvent ev) -> ev.getAuctionId()).timeWindow(Time.milliseconds(3000),Time.milliseconds(1000))
				.process(new HotBids()).setParallelism(1);

		DataStream<Tuple6<Long ,Long, Integer,Long, Long,Long>>  ty = county.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(1000))).maxBy(3);


		//DataStream<Tuple6<Long ,Long, Integer,Long, Long,Long>>  ty = county.keyBy(t->t.f0).maxBy(3);
	//	ty.writeAsCsv("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/resources/latency/ty.csv").setParallelism(1);


		DataStream<Tuple7<Long ,Long, Integer,Long, Long,Long,Long>> hotbid = ty.map(new MapFunction<Tuple6<Long ,Long, Integer,Long, Long,Long>,Tuple7<Long ,Long, Integer,Long, Long,Long, Long>>() {

			@Override
			public Tuple7<Long ,Long, Integer,Long, Long,Long,Long>map(Tuple6<Long ,Long, Integer,Long, Long,Long> t) throws Exception {

				return Tuple7.of(t.f0,t.f1,t.f2,t.f3,t.f4,t.f5,System.currentTimeMillis());
			}


		});

		hotbid.writeAsCsv("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/resources/latency/hotbid.csv").setParallelism(1);




//HighestBid
	/*	DataStream<Tuple4<Long, Double, Long, Long>>  county = file.
				keyBy((Ysb.BidEvent ev) -> ev.getAuctionId()).window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000))).
						process(new HighestBids()).setParallelism(1);

		DataStream<Tuple4<Long, Double, Long, Long>>  hourlyMax = county.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(1000))).
				maxBy(1);

		DataStream<Tuple5<Long, Double, Long, Long, Long>> Double = hourlyMax.map(new MapFunction<Tuple4<Long, Double, Long, Long>,Tuple5<Long, Double, Long, Long, Long>>() {

			@Override
			public Tuple5<Long, Double, Long, Long, Long> map(Tuple4<Long, Double, Long,Long> t) throws Exception {

				return Tuple5.of(t.f0,t.f1,t.f2,t.f3,System.currentTimeMillis());
			}


		});

		Double.writeAsCsv("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/resources/latency/highestbid.csv").setParallelism(1);*/


//Currency Conversion

	/*	DataStream<Tuple5<Long, Double,Long,Long,Long>> conversion = file.map(new MapFunction<Ysb.BidEvent, Tuple5<Long, Double,Long,Long,Long>>() {

			@Override
			public Tuple5<Long, Double,Long,Long,Long> map(Ysb.BidEvent line) throws Exception {

				Double price = line.getBidPrice()*1.24;
				long key  = line.getBidId();

				return new Tuple5(key,price,line.geteventTime(),line.getprocessingTime(),System.currentTimeMillis());
			}

		});


		conversion.writeAsCsv("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/resources/latency/ccurrencyconversion.csv").setParallelism(1);*/






		//concat.writeAsCsv("/home/nabil/eclipse-workspace/MovieRatingFlink/src/main/java/movie/ratings/calculateYsbRatings.csv").setParallelism(1);




		env.execute("Flink Streaming Java API Skeleton");

		System.out.println("Throughput: " + (System.currentTimeMillis() - startTime));

	}



	public static class HotBids extends ProcessWindowFunction<
			Ysb.BidEvent, Tuple6<Long,Long, Integer, Long, Long, Long>, Long, TimeWindow> {

		@Override
		public void process(Long key, Context context, Iterable<Ysb.BidEvent> bids, Collector<Tuple6<Long,Long, Integer, Long, Long,Long>> out) throws Exception {
			long sumOfBids = 0;
			long eventTime = 0, processingTime = 0;
			int bidId = 0;
			for (Ysb.BidEvent f : bids) {
				bidId = f.getBidId();
				sumOfBids ++;
				eventTime = f.geteventTime();
				processingTime = f.getprocessingTime();
			}

			out.collect(Tuple6.of(context.window().getEnd(),key,bidId, sumOfBids, eventTime, processingTime));
		}
	}

	public static class HighestBids extends ProcessWindowFunction<
			Ysb.BidEvent, Tuple4<Long, Double, Long, Long>, Long, TimeWindow> {

		@Override
		public void process(Long key, Context context, Iterable<Ysb.BidEvent> bids, Collector<Tuple4<Long, Double, Long, Long>> out) throws Exception {
			double bidPrice = 0;
			long eventTime = 0, processingTime = 0;
			for (Ysb.BidEvent f : bids) {
				if(f.getBidPrice() > bidPrice){
					bidPrice = f.getBidPrice();
					eventTime = f.geteventTime();
					processingTime = f.getprocessingTime();
				}

			}
			out.collect(Tuple4.of(key,bidPrice, eventTime, processingTime));
		}
	}


}
