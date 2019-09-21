package com.org.services

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//spark 1.6 version

object BookingService {
  
  def main(args: Array[String]): Unit= {
    
    //Loading the context
    val conf = new SparkConf().setAppName("BookingDetails").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    //Load the data
    val bookingDetails = sqlContext.read.json(args(0))
    val transactionDetails = sqlContext.read.json(args(1))
	
	
	// Rename column names for reusability
	val bd_destination = bookingDetails("destination")
	val bd_buId = bookingDetails("buId")
	
	val ts_logId = transactionDetails("logId")
	val ts_buId = transactionDetails("buId")
	val ts_price = transactionDetails("price")
	val ts_datetime = transactionDetails("datetime")
	
	
	//Sort the Transaction details in descending order (such that latest transaction will be in the top)
	 val transactionDetails_latest = transactionDetails.sort( transactionDetails("datetime").desc)
	
	//Join the RDD based on buId and drop ts_buId for the required format
	val bookingTransactions  = bookingDetails.join(transactionDetails_latest, bd_buId === ts_buId).drop(ts_buId)
	
	//Reordering the columns 
	val bookingTransactions_sorted = bookingTransactions.select(ts_logId,bd_buId,ts_price,ts_datetime,bd_destination)
	
	// downsizing the output files
	 val bookingTransactions_sorted_singleFile = bookingTransactions_sorted.coalesce(1)
	 
	 //saving the data in specific path
	 bookingTransactions_sorted_singleFile.write.json("/Users/giriadini/Desktop/output/Booking_Transcations")
	 
  }
}
