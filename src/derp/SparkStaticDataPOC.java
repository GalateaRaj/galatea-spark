package derp;

import org.apache.spark.api.java.JavaSparkContext;

import derp.PositionUtils.Position;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkStaticDataPOC {

	public static void main(String[] args) {
		// JavaSparkContext ctx = new JavaSparkContext(new
		// SparkConf().setAppName("Derp")
		// .setMaster("spark://172.16.0.7:7077").set("spark.driver.host",
		// "172.16.1.107"));

		try (
				JavaSparkContext ctx = new JavaSparkContext(
						new SparkConf().setAppName("Derp").setMaster("local[2]"));) {

			int partitions = 2;

			// Create my input data
			List<Position> posList = PositionUtils.createTestPositions();

			// Partition and store the data in my local spark cluster
			JavaRDD<Position> posRdd = ctx.parallelize(posList, partitions);

			long startTime = System.currentTimeMillis();
			long result = posRdd.map(s -> s.acct.equals("Cust1") ? new Position(s.acct, s.inst, 0l) : s)
					.filter(s -> s.qty >= 100000).count();

			// long result = posRdd.count(); // rdd.filter(s -> s >= 4).count();

			System.out.println("Count of positions: "+result);

			// JavaRDD<Integer> rdd2 = ctx.parallelize(l, partitions);
			//
			// long result2 = rdd2.count(); // rdd.filter(s -> s >= 4).count();
			//
			// System.out.println(result2);

			// long result = rdd.filter(s -> s >= 4).count();

			// map(s -> s>5 ? -1 : s)

			// long result = rdd.aggregate(0, new Function2<Integer, Integer,
			// Integer>() {
			//
			// private static final long serialVersionUID =
			// 1367367275637796745L;
			//
			// @Override
			// public Integer call(Integer v1, Integer v2) throws Exception {
			// return v1 + v2;
			// }
			// }, new Function2<Integer, Integer, Integer>() {
			//
			// private static final long serialVersionUID =
			// 3012584917696136787L;
			//
			// @Override
			// public Integer call(Integer v1, Integer v2) throws Exception {
			// return v1 + v2;
			// }
			//
			// });

			System.out.println("Completion time:" + (System.currentTimeMillis() - startTime));
		}
	}

}
