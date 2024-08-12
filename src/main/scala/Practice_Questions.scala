import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, col, column, count, date_format, dense_rank, initcap, lag, lead, max, min, month, rank, row_number, sum, sumDistinct, when, year}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.month
object Practice_Questions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Practice_Questions")
      .config("spark.master", "local")
      .getOrCreate()

//    val Employeedata = StructType(List(
//      StructField("Id", IntegerType),
//      StructField("Name", StringType),
//      StructField("Score", IntegerType),
//      StructField("Order_id", IntegerType),
//      StructField("Order_date", StringType)
//    ))
    import spark.implicits._

    //Question_1

    val df = List(
      ("E001", "Sales", 85, "2024-02-10", "Sales Manager"),
      ("E002", "HR", 78, "2024-03-15", "HR", "Assistant"),
      ("E003", "IT", 92, "2024-01-22", "IT", "Manager"),
      ("E004", "Sales", 88, "2024-02-18", "Sales Rep"),
      ("E005", "HR", 95, "2024-03-20", "HR Manager")
    ).toDF("employee_id", "department", "performance_score", "review_date", "position")

    val df1 = df.withColumn("Review_month", month(col("review_date")))
    df1.show()
  }

}
