//Ronan Murphy 15397831 12/11/2019
//Assignment 4  - Twitter2D
package assignment4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
public class KmeansTwitter {
	public static void main(String args[]) {
		
		//set inital values for K-clusters and amount of iterations for K-means Clustering
		int k = 4;
		int numIterations = 20;
		
		//set up directory, spark configuration and context for the application
		System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf conf = new SparkConf().setAppName("K-means twitter").
				setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//create path to locate text file and read it through String Java rdd stream
		String path = "C:/Spark/twitter2D.txt";
		JavaRDD<String> data = sc.textFile(path);
		
		//create RDD pair of string and vector and map to the values of the text file
		JavaPairRDD<String, Vector> parsedData =
	        data.mapToPair(
	                (String s) -> {
	                	//add string of the text file as first pair member split by comma
	                    String[] parts = s.split(",");
	                     //create new array of double values to add latitude and longitude of each location 
	                    double[] values = new double[2];
	                    //for each line will add first two values and parse as doubles
	    				for (int i = 0; i < 2; i++) {
	    					values[i] = Double.parseDouble(parts[i]);
	    				}
	    				//return pair - the string of text file, vector of doubles values convert string to vector
	    				//vector is needed to input for cluster model
	    				return new Tuple2<>(s, Vectors.dense(values));			
	    				}
	                );
		  //train K-means cluster model with the double values collected in 
		  //the tuple separating into 4 clusters for 20 iterations
		  KMeansModel clusters = KMeans.train(parsedData.values().rdd(), k, numIterations);
		  
		  //predict the cluster of each tweet location and add to tuple pair which holds cluster number
		  //and string of the tweets text
		  JavaPairRDD<Integer, String> result = parsedData.mapToPair(mapper ->{
			  String[] parts = mapper._1.split(","); 
			  //if line has extra members takes last value as tweet name
			  if (parts.length > 5)
				  parts[4] = parts[5];
			  return new Tuple2<>(clusters.predict(mapper._2), parts[4]);
		  });
		  
		  
		  //sort the predicted results by their cluster and print the tweet text with cluster number
		  for (Tuple2<Integer, String> out: result.sortByKey().collect()) {
			  System.out.println("Tweet '" + out._2 + "' is in Cluster " + out._1);
		  }
		  
	}
}	