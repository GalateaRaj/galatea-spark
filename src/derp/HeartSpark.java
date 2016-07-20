package derp;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.SparkConf;

public class HeartSpark {

	public static class Position implements Serializable {
		private static final long serialVersionUID = 7904127661383626801L;

		String acct;
		String inst;
		Long qty;

		public Position(final String acct_, final String inst_, final Long qty_) {
			acct = acct_;
			inst = inst_;
			qty = qty_;
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// JavaSparkContext ctx = new JavaSparkContext(new
		// SparkConf().setAppName("Derp")
		// .setMaster("spark://172.16.0.7:7077").set("spark.driver.host",
		// "172.16.1.107"));

		//Create a local, single jvm spark context
		JavaSparkContext ctx = new JavaSparkContext(new SparkConf().setAppName("Derp").setMaster("local[2]"));

		int partitions = 2;
		int custCount = 100;
		int instCount = 10000;
		int posCount = 1000000;
		
		//Create my input data
		 List<Position> posList = new ArrayList<Position>(posCount);
		 for(int i=0;i<posCount;i++) {
			 String cust = "Cust-"+ThreadLocalRandom.current().nextInt(0, custCount);
			 String inst = "Inst-"+ThreadLocalRandom.current().nextInt(0, instCount);
			 long qty = ThreadLocalRandom.current().nextInt(0, 500000); 
			 
			 posList.add(new Position(cust,inst,qty));
		 }
				 				
		 //Partition and store the data in my local spark cluster
		 JavaRDD<Position> posRdd = ctx.parallelize(posList, partitions);

		 long startTime = System.currentTimeMillis();
		 long result = posRdd.map(s -> s.acct.equals("Cust1") ? new
		 Position(s.acct,s.inst,0l) : s).filter(s -> s.qty >= 100000).count();

		 
		//long result = posRdd.count(); // rdd.filter(s -> s >= 4).count();

		 
		System.out.println(result);

//		JavaRDD<Integer> rdd2 = ctx.parallelize(l, partitions);
//
//		long result2 = rdd2.count(); // rdd.filter(s -> s >= 4).count();
//
//		System.out.println(result2);

		// long result = rdd.filter(s -> s >= 4).count();

		// map(s -> s>5 ? -1 : s)

		// long result = rdd.aggregate(0, new Function2<Integer, Integer,
		// Integer>() {
		//
		// private static final long serialVersionUID = 1367367275637796745L;
		//
		// @Override
		// public Integer call(Integer v1, Integer v2) throws Exception {
		// return v1 + v2;
		// }
		// }, new Function2<Integer, Integer, Integer>() {
		//
		// private static final long serialVersionUID = 3012584917696136787L;
		//
		// @Override
		// public Integer call(Integer v1, Integer v2) throws Exception {
		// return v1 + v2;
		// }
		//
		// });

		System.out.println("done "+ (System.currentTimeMillis()-startTime));

		ctx.stop();

	}

}
