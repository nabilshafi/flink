package movie.ratings;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Timestamp;


public class Ysb {

    public static class BidEvent implements Serializable {

        public long eventTime;
        public long auctionId;
        public int personId;
        public int bidId;
        public double bidPrice;
        public long processingTime;

        public BidEvent() {
        }

        public BidEvent(long auctionId, long eventTime, int personId, int bidId, double bidPrice, long processingTime) {
            this.eventTime = eventTime;
            this.auctionId = auctionId;
            this.personId = personId;
            this.bidId = bidId;
            this.bidPrice = bidPrice;
            this.processingTime = processingTime;
        }

        public Long geteventTime() {
            return eventTime;
        }
        public Long getprocessingTime() {
            return processingTime;
        }

        public Long getAuctionId() {
            return auctionId;
        }

        public Integer getPersonId() {
            return personId;
        }

        public Integer getBidId() {
            return bidId;
        }

        public Double getBidPrice() {
            return bidPrice;
        }
    }



    public static class MovieRating implements Serializable {


        public long userId;
        public long movieId;
        public long eventTime;
        public double rating;
        public long processingTime;

        public long getUserId() {
            return userId;
        }

        public long getMovieId() {
            return movieId;
        }

        public long getEventTime() {
            return eventTime;
        }

        public double getRating() {
            return rating;
        }

        public long getProcessingTime() {
            return processingTime;
        }

        public MovieRating() {
        }

        public MovieRating(long eventTime, long userId, long movieId, double rating, long processingTime) {
            this.eventTime = eventTime;
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.processingTime = processingTime;
        }


    }

    public static class AuctionEvent implements Serializable {

        public long timestamp;
        public long auctionId;
        public long personId;
        public long itemId;
        public double initialPrice;
        public double quantity;
        public long start;
        public long end;

        public long getCategoryId() {
            return categoryId;
        }

        public void setCategoryId(long categoryId) {
            this.categoryId = categoryId;
        }

        public long categoryId;
        public long ingestionTimestamp;


        public AuctionEvent(long timestamp, long auctionId, long itemId, long personId, double initialPrice, long categoryID, double quantity, long start, long end) {

            this.timestamp = timestamp;
            this.auctionId = auctionId;
            this.personId = personId;
            this.itemId = itemId;
            this.initialPrice = initialPrice;
            this.categoryId = categoryID;
            this.start = start;
            this.end = end;
            this.quantity = quantity;
            this.ingestionTimestamp = ingestionTimestamp;
        }

    }




    public static class YSBRecord implements Serializable {

        public String getPageId() {
            return pageId;
        }

        public String getAdvertisingId() {
            return advertisingId;
        }

        public String getAdvertisingType() {
            return advertisingType;
        }

        public String getEventType() {
            return eventType;
        }

        public Timestamp getEventTime() {
            return eventTime;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public final String userId;
        public final String pageId;
        public final String advertisingId;
        public final String advertisingType;
        public final String eventType;
        public final Timestamp eventTime;
        public final String ipAddress;

        public YSBRecord(String userId, String pageId, String advertisingId, String advertisingType, String eventType, Timestamp eventTime, String ipAddress) {
            this.userId = userId;
            this.pageId = pageId;
            this.advertisingId = advertisingId;
            this.advertisingType = advertisingType;
            this.eventType = eventType;
            this.eventTime = eventTime;
            this.ipAddress = ipAddress;
        }

        public String getUserId() {
            return userId;
        }
    }

    public static class YSBSource extends RichParallelSourceFunction<BidEvent> {

        private static final String DELIMITER = "\n";

        private transient Socket currentSocket;

        private volatile boolean isRunning = true;

        private String hostname = "localhost";
        private int port = 5000;

        private static final int CONNECTION_TIMEOUT_TIME = 0;

        private volatile boolean running = true;


        private transient MappedByteBuffer mbuff;

        private transient FileChannel channel;

        private BufferedReader reader,in;
        private PrintWriter output;


        public YSBSource() throws IOException {

        }


        @Override
        public void close() throws Exception {
            currentSocket.close();

            //channel.close();
        }

        @Override
        public void run(SourceContext<BidEvent> ctx) throws Exception {
            final StringBuilder buffer = new StringBuilder();

            //172.16.0.254
            currentSocket =  new Socket("192.168.1.10", 5000);


               // socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);


        in =  new BufferedReader(
                new InputStreamReader(currentSocket.getInputStream()));
        output =  new PrintWriter(currentSocket.getOutputStream(), true);

                // Necessary, so port stays open after disconnect
                output.println( 0 + ":bids");

                //output.print(getRuntimeContext().getIndexOfThisSubtask() + ":persons\n");

                String line = null;
                try {
                    int i = 0;
                    while ((line = in.readLine()) != null) {

                       // line += "," + System.currentTimeMillis();
                        String[] str = line.split(",");
                        ctx.collect(new BidEvent(Long.parseLong(str[0]), Long.parseLong(str[1]),Integer.parseInt(str[2]),
                                Integer.parseInt(str[3]), Double.parseDouble(str[4]), System.currentTimeMillis() ));
                    }
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                }



        }

        @Override
        public void cancel() {


            running = false;
        }
    }


    public static class SocketSource extends RichParallelSourceFunction<BidEvent> {

        private static final String DELIMITER = "\n";

        private transient Socket currentSocket;

        private volatile boolean isRunning = true;

        private String hostname = "localhost";
        private int port = 5000;

        private static final int CONNECTION_TIMEOUT_TIME = 0;

        private volatile boolean running = true;


        private transient MappedByteBuffer mbuff;

        private transient FileChannel channel;

        private BufferedReader reader,in;
        private PrintWriter output;


        public SocketSource() throws IOException {

        }


        @Override
        public void close() throws Exception {
            currentSocket.close();

            //channel.close();
        }

        @Override
        public void run(SourceContext<BidEvent> ctx) throws Exception {
            final StringBuilder buffer = new StringBuilder();

            //172.16.0.254
            currentSocket =  new Socket("192.168.0.10", 5000);


            // socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);


            in =  new BufferedReader(
                    new InputStreamReader(currentSocket.getInputStream()));
            output =  new PrintWriter(currentSocket.getOutputStream(), true);

            // Necessary, so port stays open after disconnect
            output.println( 0 + ":bids");

            //output.print(getRuntimeContext().getIndexOfThisSubtask() + ":persons\n");

            String line = null;
            try {
                int i = 0;
                while ((line = in.readLine()) != null) {

                    line += "," + System.currentTimeMillis();
                    String[] str = line.split(",");
                    ctx.collect(new BidEvent(Long.parseLong(str[0]), Long.parseLong(str[1]),Integer.parseInt(str[2]),
                            Integer.parseInt(str[3]), Double.parseDouble(str[4]), Long.parseLong(str[5])));
                }
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }



        }

        @Override
        public void cancel() {


            running = false;
        }
    }

}
