package derp;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

public class SparkStructuredStreamingPOC {

	public static int PORT = 9999;
	public static String[] INPUT = {"Hello world","hello","crap","Jon","Spark sucks","Say hello to my little friend"};
	public static long SLEEP_TIME = 30;
	
	//MAKE SURE HDFS IS RUNNING BEFORE YOU RUN THIS!
	public static void main(String[] args) {

		// Set both of these properties appropriately
		System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.1\\");
		String sparkWarehouseDir = "file:///C://";

		//This will fire off a thread that will periodically send data into the stream via a socket
		startSocketListener();
		
		//Start my session
		SparkSession spark = SparkSession.builder().appName("Derp").config("spark.sql.warehouse.dir", sparkWarehouseDir)
				.master("local[2]").getOrCreate();

		//Define my input stream
		Dataset<String> lines = spark.readStream().format("socket").option("host", "localhost").option("port", PORT)
				.load().as(Encoders.STRING());

		// Split the lines into words
		Dataset<String> words = lines.as(Encoders.STRING()).flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		}, Encoders.STRING());

		//Aggregate
		Dataset<Row> wordCounts = words.groupBy("value").count();

		//Define my output and run continuous query
		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

		query.awaitTermination();
	}

	public static void startSocketListener() {

		System.out.println("Starting socket listener");
		
		new Thread(() -> {
			try (ServerSocket serverSocket = new ServerSocket(PORT);
					Socket clientSocket = serverSocket.accept();
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
					BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));) {
				System.out.println("Connected....");

				while (true) {
					String words =  INPUT[ThreadLocalRandom.current().nextInt(0, INPUT.length)].toLowerCase();
					System.out.println("sending output: "+words);
					out.println(words);
					System.out.println("Sleeping for "+SLEEP_TIME+"(s)");
					Thread.sleep(SLEEP_TIME * 1000);
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}).start();
	}

}
