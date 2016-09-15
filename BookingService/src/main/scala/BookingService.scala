
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object BookingCustomer{


def main(args:Array[String]]){

 val conf = new SparkConf().setAppName("BookingCustomer").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val bookingDetails = sqlContext.read.json("f1.json")
	
	val bd_destination = bookingDetails("destination")
	val bd_buId = bookingDetails("buId")


	// val filtervalues = bookingDetails.filter(bd_destination_column.!==("") && bd_buId_column.>("40"))
	
	val bd_destination_filter = bookingDetails.filter(bd_destination.!==("") )

	 
	 val transactionDetails = sqlContext.read.json("f2.json")
	 
	val ts_logId = transactionDetails("logId")
	val ts_buId = transactionDetails("buId")
	val ts_price = transactionDetails("price")
	val ts_datetime = transactionDetails("datetime")
	
	
	//val bookingTransactions  = bookingDetails.join(transactionDetails, bookingDetails.col("buId") === transactionDetails.col("buId"))

	val bookingTransactions  = bd_destination_filter.join(transactionDetails)

	bookingTransactions.show()

}
