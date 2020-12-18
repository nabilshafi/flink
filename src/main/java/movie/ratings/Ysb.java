package movie.ratings;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Timestamp;


public class Ysb {



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

    public static class YSBSource extends RichParallelSourceFunction<YSBRecord> {

        private static final String DELIMITER = "\n";

        private transient Socket currentSocket;

        private volatile boolean isRunning = true;

        private String hostname = "localhost";
        private int port = 31000;

        private static final int CONNECTION_TIMEOUT_TIME = 0;

        private volatile boolean running = true;


        private transient MappedByteBuffer mbuff;

        private transient FileChannel channel;



        public YSBSource() {

        }

/*
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            int idx = getRuntimeContext().getIndexOfThisSubtask();

            channel = FileChannel.open(new File(path + "/ysb" + idx + ".bin").toPath(), StandardOpenOption.READ);
            mbuff = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
*/


        @Override
        public void close() throws Exception {
            channel.close();
        }

        @Override
        public void run(SourceContext<YSBRecord> ctx) throws Exception {
            final StringBuilder buffer = new StringBuilder();

            try (Socket socket = new Socket()) {
                currentSocket = socket;

                socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);

                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintStream output = new PrintStream(socket.getOutputStream(), true);

                // Necessary, so port stays open after disconnect
                output.println( 0 + ":yahoo");

                //output.print(getRuntimeContext().getIndexOfThisSubtask() + ":persons\n");
                char[] cbuf = new char[8192];
                int bytesRead;
                while (isRunning) {

                    try {
                        if ((bytesRead = reader.read(cbuf)) == -1) {
                            break;
                        }
                    } catch (IOException exception) {
                        break;
                    }

                    buffer.append(cbuf, 0, bytesRead);
                    int delimPos;
                    while (buffer.length() >= DELIMITER.length() && (delimPos = buffer.indexOf(DELIMITER)) != -1) {
                        String record = buffer.substring(0, delimPos);
                        // truncate trailing carriage return
                        if (record.endsWith("\r")) {
                            record = record.substring(0, record.length() - 1);
                        }

                        synchronized (ctx.getCheckpointLock()) {

                            if (!isRunning) {
                                return;
                            }

                            String[] str = record.split(",");
                            ctx.collect(new YSBRecord(str[0], str[1], str[2], str[3], str[4],new Timestamp(Long.valueOf(str[5])), str[6])); // filtering is possible also here but it d not be idiomatic

                        }

                        buffer.delete(0, delimPos + DELIMITER.length());
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
