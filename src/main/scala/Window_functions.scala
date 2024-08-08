import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, column, count, dense_rank, initcap, lag, lead, max, min, month, rank, row_number, sum, sumDistinct, when, year}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Window_functions {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DF_Ques_1")
      .config("spark.master", "local")
      .getOrCreate()

    val employeesDF = StructType(List(
      StructField("Id", IntegerType),
      StructField("Name", StringType),
      StructField("Score", IntegerType),
      StructField("Order_id", IntegerType),
      StructField("Order_date", StringType)
    ))
    import spark.implicits._

//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category2", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category3", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category3", 180)
//    ).toDF("Product", "Category", "Revenue")
//
//    val window = Window.partitionBy("Product")
//    val df = salesData.withColumn("total_revenue", sum(col("Revenue")).over(window))
//    df.show()

//    val ratingData = List(
//      ("User1", "Movie1", 4.5),
//      ("User1", "Movie2", 3.5),
//      ("User1", "Movie3", 2.5),
//      ("User1", "Movie4", 4.0),
//      ("User1", "Movie5", 3.0),
//      ("User1", "Movie6", 4.5),
//      ("User2", "Movie1", 3.0),
//      ("User2", "Movie2", 4.0),
//      ("User2", "Movie3", 4.5),
//      ("User2", "Movie4", 3.5),
//      ("User2", "Movie5", 4.0),
//      ("User2", "Movie6", 3.5)
//    ).toDF("User", "Movie", "Rating")
//
//    val window = Window.partitionBy("User").orderBy("Rating")
//    val df = ratingData.withColumn("leadnewcolumn",lag("Rating",1).over(window))
//    df.show()
//
//    val df = List(
//      (1, "KitKat",1000.0,"2021-01-01"),
//      (1, "KitKat",2000.0,"2021-01-02"),
//      (1, "KitKat",1000.0,"2021-01-03"),
//      (1, "KitKat",2000.0,"2021-01-04"),
//      (1, "KitKat",3000.0,"2021-01-05"),
//      (1, "KitKat",1000.0,"2021-01-06")
//    ).toDF("IT_ID","IT_name","Price","Pricedata")
//
//    val window = Window.partitionBy("IT_ID").orderBy("Pricedata")
//    val df1 = df.withColumn("leadnewcolumn",col("Price")-lag(col("Price"),1).over(window))
//    df1.show()

//    val shopData = List(
//      (1, "KitKat", 1000.0,"2021-01-01"),
//      (1, "KitKat", 2000.0,"2021-01-02"),
//      (1, "KitKat", 1000.0,"2021-01-03"),
//      (1, "KitKat", 2000.0,"2021-01-04"),
//      (1, "KitKat", 3000.0,"2021-01-05"),
//      (1, "KitKat", 1000.0,"2021-01-06")
//    ).toDF("IT_ID", "IT_Name", "Price","PriceDate")
//
//    val window =  Window.partitionBy("IT_ID").orderBy("Price")
////    val df = shopData.withColumn("leadnewcolumn",rank().over(window))
////val df = shopData.withColumn("leadnewcolumn",dense_rank().over(window))
//val df = shopData.withColumn("leadnewcolumn",row_number().over(window))
//    df.show()

    //Question_20(Set_B)


    //Question_19(Set_B)
//    val df = List(
//      (1, "Bug", 1.5, "High"),
//      (2, "Feature", 3.0, "Medium"),
//      (3, "Bug", 4.5, "Low"),
//      (4, "Bug", 2.0, "High"),
//      (5, "Enhancement", 1.0, "Medium"),
//      (6, "Bug", 5.0, "Low")
//    ).toDF("ticket_id", "issue_type", "resolution_time", "priority")
//    val df1 = df.select(col("ticket_id"),col("issue_type"),col("resolution_time"),col("priority"),
//      when(col("resolution_time")<=2,"Quick").when(col("resolution_time")>2,"Moderate").
//        otherwise("Slow").alias(("Resolution_status")))
////    val df2 = df.filter(col("issue_type").contains("Bug"))
//    val window = Window.partitionBy("Resolution_status")
//    val df3 = df1.withColumn("Sum",sum("resolution_time").over(window))
//    val df4 = df3.withColumn("Avg",avg("resolution_time").over(window))
//    val df5 = df4.withColumn("Max",max("resolution_time").over(window))
//    val df6 = df5.withColumn("Min",min("resolution_time").over(window))
//    df6.show()

    //Question_18(Set_B)
//    val df = List(
//      (1, "Sales Department", "2500", "2024-01-10"),
//      (2, "Marketing Department", "1500", "2024-01-15"),
//      (3, "IT Department", "800", "2024-01-20"),
//      (4, "HR Department", "1200", "2024-02-01"),
//      (5, "Sales Department", "1800", "2024-02-10"),
//      (6, "IT Department", "950", "2024-03-01")
//    ).toDF("employee_id", "department", "bonus", "bonus_date")
//    val df1 = df.select(col("employee_id"),col("department"),col("bonus"),col("bonus_date"),
//      when(col("bonus")>2000,"High").when(col("bonus")>=1000 && col("bonus")<=2000,"Medium")
//        .otherwise("Low").alias(("bonus_category")))
////    val df2 = df.filter(col("department").endsWith("Department"))
//    val window = Window.partitionBy("bonus_category")
//    val df3 = df1.withColumn("Sum",sum("bonus").over(window))
//    val df4 = df3.withColumn("Avg",avg("bonus").over(window))
//    val df5 = df4.withColumn("Min",min("bonus").over(window))
//    val df6 = df5.withColumn("Max",max("bonus").over(window))
//    df6.show()



    //Question_17(Set-B)
//    val df = List(
//      (1, "New Website", "50000", "55000"),
//      (2, "Old Software", "30000", "25000"),
//      (3, "New App", "40000", "40000"),
//      (4, "New Marketing", "15000", "10000"),
//      (5, "Old Campaign", "20000", "18000"),
//      (6, "New Research", "60000", "70000")
//    ).toDF("project_id", "project_name", "budget", "spent_amount")
//    val df1 = df.select(col("project_id"),col("project_name"),col("budget"),col("spent_amount"),
//      when(col("spent_amount") > col("budget"), "Over_Budget").when(col("spent_amount") === col("budget"), "On_Budget")
//        .otherwise("Under_Budget").alias(("Budget_status")))
////    val df2 = df.filter(col("project_name").startsWith("New"))df2.show()
//    val window = Window.partitionBy("Budget_status")
//    val df3 = df1.withColumn("Sum",sum("spent_amount").over(window))
//    val df4 = df3.withColumn("Avg",avg("spent_amount").over(window))
//    val df5 = df4.withColumn("Min",min("spent_amount").over(window))
//    val df6 = df5.withColumn("Max",max("spent_amount").over(window))
//    df6.show()

    //Question_16(Set-B)
//    val df = List(
//      (1, "John Smith", "15000", "12000"),
//      (2, "Jane Doe", "9000", "10000"),
//      (3, "John Doe", "5000", "6000"),
//      (4, "John Smith", "13000", "13000"),
//      (5, "Jane Doe", "7000", "7000"),
//      (6, "John Doe", "8000", "8500")
//    ).toDF("sales_id", "sales_rep", "sales_amount", "target_amount")
//    val df1 = df.select(col("sales_id"),col("sales_rep"),col("sales_amount"),col("target_amount"),
//      when(col("sales_amount")>=col("target_amount"),"Above Target").otherwise("Below Target")
//        alias(("Achievement_Status")))
////    val df2 = df.filter(col("sales_rep").contains("John"))df2.show()
//    val window = Window.partitionBy("Achievement_Status")
////    val df3 = df1.withColumn("Sum",sum("sales_amount").over(window))
////    val df4 = df3.withColumn("Avg",avg("sales_amount").over(window))
//    val df5 = df1.withColumn("Max",max("sales_amount").over(window))
//    df5.show()
//    val df6 = df5.withColumn("Min",min("sales_amount").over(window))
//    df6.show()

    //Question_15(Set-B)
//    val df = List(
//      (1, 1, "700", "2024-02-05"),
//      (2, 2, "150", "2024-02-10"),
//      (3, 3, "400", "2024-02-15"),
//      (4, 4, "600", "2024-02-20"),
//      (5, 5, "250", "2024-02-25"),
//      (6, 6, "1000", "2024-02-28")
//    ).toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")
//    val df1 = df.select(col("purchase_id"),col("customer_id"),col("purchase_amount"),col("purchase_date"),
//      when(col("purchase_amount")>500,"Large").when(col("purchase_amount")>=200 && col("purchase_amount")<=500
//      ,"Medium").otherwise("Small").alias(("Purchase_status")))
////    val df2 = df.filter(col("purchase_date").contains("2024-02"))df2.show()
//    val window = Window.partitionBy("Purchase_status")
//    val df3 = df1.withColumn("Sum",sum("purchase_amount").over(window))
//    val df4 = df3.withColumn("Avg",avg("purchase_amount").over(window))
//    val df5 = df4.withColumn("Max",max("purchase_amount").over(window))
//    val df6 = df5.withColumn("Min",min("purchase_amount").over(window))
//    df6.show()

    //Question_14(Set-B)
//    val df = List(
//      (1, "Asia", "15000", "2024-01-10"),
//      (2, "Europe", "6000", "2024-01-15"),
//      (3, "Asia", "3000", "2024-02-20"),
//      (4, "Asia", "20000", "2024-02-25"),
//      (5, "North America", "4000", "2024-03-05"),
//      (6, "Asia", "8000", "2024-03-12")
//    ).toDF("shipment_id", "destination", "shipment_value", "shipment_date")
//    val df1 = df.select(col("shipment_id"),col("destination"),col("shipment_value"),col("shipment_date"),
//      when(col("shipment_value")>10000,"High").when(col("shipment_value")>=5000 &&
//        col("shipment_value")<=10000,"Medium").otherwise("Low").alias(("Value_category")))
// //   val df2 = df.filter(col("destination").contains("Asia"))df2.show()
//    val window = Window.partitionBy("Value_category")
//    val df3 = df1.withColumn("Sum",sum("shipment_value").over(window))
//    val df4 = df3.withColumn("Avg",avg("shipment_value").over(window))
//    val df5 = df4.withColumn("Max",max("shipment_value").over(window))
//    val df6 = df5.withColumn("Min",min("shipment_value").over(window))
//    df6.show()

    //Question_13(Set-B)
//    val df = List(
//      (1, "IT", "130000", "2024-01-10"),
//      (2, "HR", "80000", "2024-01-15"),
//      (3, "IT", "60000", "2024-02-20"),
//      (4, "IT", "70000", "2024-02-25"),
//      (5, "Sales", "50000", "2024-03-05"),
//      (6, "IT", "90000", "2024-03-12")
//    ).toDF("employee_id", "department", "salary", "last_increment_date")
//    val df1 = df.select(col("employee_id"),col("department"),col("salary"),col("last_increment_date"),
//      when(col("salary")>120000,"High").when(col("salary")>=60000 && col("salary")<=120000,"Medium").
//        otherwise("Low").alias(("Salary_band")))
// //   val df2 = df.filter(col("department").startsWith("IT")) df2.show()
//    val window =  Window.partitionBy("Salary_band")
//    val df3 = df1.withColumn("Sum",sum("salary").over(window))
//    val df4 = df3.withColumn("Avg",avg("salary").over(window))
//    val df5 = df4.withColumn("Max",max("salary").over(window))
//    val df6 = df5.withColumn("Min",min("salary").over(window))
//    df6.show()

    //Question_12
//    val df = List(
//      (1, "Action Hero", "2024-01-10", 8),
//      (2, "Comedy Nights", "2024-01-15", 25),
//      (3, "Action Packed", "2024-01-20", 55),
//      (4, "Romance Special", "2024-02-01", 5),
//      (5, "Action Force", "2024-02-10", 45),
//      (6, "Drama Series", "2024-03-01", 70)
//    ).toDF("show_id", "movie_title", "showtime", "seats_available")
//    val df1 = df.select(col("show_id"),col("movie_title"),col("showtime"),col("seats_available"),
//      when(col("seats_available")<=10,"Full").when(col("seats_available")>=11 && col("seats_available")<=50,"Limited")
//        .otherwise("Plenty").alias(("Availability")))
// //   val df2 = df.filter(col("movie_title").contains("Action"))df2.show()
//    val window = Window.partitionBy("Availability")
//    val df3 = df1.withColumn("Sum",sum("seats_available").over(window))
//    val df4 = df3.withColumn("Avg",avg("seats_available").over(window))
//    val df5 = df4.withColumn("Max",max("seats_available").over(window))
//    val df6 = df5.withColumn("Min",min("seats_available").over(window))
//    df6.show()

    //Question_11
//    val df = List(
//      (1, "The Great Gatsby", "150", "2024-01-10"),
//      (2, "The Catcher in the Rye", "80", "2024-01-15"),
//      (3, "Moby Dick", "200", "2024-01-20"),
//      (4, "To Kill a Mockingbird", "30", "2024-02-01"),
//      (5, "The Odyssey", "60", "2024-02-10"),
//      (6, "War and Peace", "20", "2024-03-01")
//    ).toDF("book_id", "book_title", "stock_quantity", "last_updated")
//    val df1 = df.select(col("book_id"),col("book_title"),col("stock_quantity"),col("last_updated"),
//      when(col("stock_quantity")>100,"High").when(col("stock_quantity")>=50 && col("stock_quantity")<=100,"Medium")
//        .otherwise("Low").alias(("Stock_level")))
////    val df2 = df.filter(col("book_title").startsWith("T")df2.show()
//    val window = Window.partitionBy("Stock_level")
//    val df3 = df1.withColumn("Sum",sum("stock_quantity").over(window))
//    val df4 = df3.withColumn("Avg",avg("Stock_quantity").over(window))
//    val df5 = df4.withColumn("Max",max("Stock_quantity").over(window))
//    val df6 = df5.withColumn("Min",min("Stock_quantity").over(window))
//    df6.show()

    //Question_10
//    val df = List(
//      (1, "2024-01-10", 9, "Sick"),
//      (2, "2024-01-11", 7, "Scheduled"),
//      (3, "2024-01-12", 8, "Sick"),
//      (4, "2024-01-13", 4, "Scheduled"),
//      (5, "2024-01-14", 6, "Sick"),
//       (6, "2024-01-15", 8, "Scheduled")
//    ).toDF("employee_id", "attendance_date", "hours_worked", "attendance_type")
//    val df1 = df.select(col("employee_id"),col("attendance_date"),col("hours_worked"),col("attendance_type"),
//      when(col("hours_worked")>=8,"Fullday").otherwise("Halfday").alias(("Attendance_status")))
// //   val df2 = df.filter(col("attendance_type").startsWith("S"))
//    val window = Window.partitionBy("Attendance_status")
//    val df3 = df1.withColumn("Sum",sum("hours_worked").over(window))
//    val df4 = df3.withColumn("Avg",avg("hours_worked").over(window))
//    val df5 = df4.withColumn("Max",max("hours_worked").over(window))
//    val df6 = df5.withColumn("Min",min("hours_worked").over(window))
//    df6.show()


    //Question_9(Set_B)
//    val df = List(
//      (1, 1, "2500", "2024-01-05"),
//      (2, 2, "1500", "2024-01-15"),
//      (3, 3, "500", "2024-02-20"),
//      (4, 4, "2200", "2024-03-01"),
//      (5, 5, "900", "2024-01-25"),
//      (6, 6, "3000", "2024-03-12")
//    ).toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")
//    val df1 = df.select(col("purchase_id"),col("customer_id"),col("purchase_amount"),col("purchase_date"),
//      when(col("purchase_amount")>2000,"Large").when(col("purchase_amount")>=1000 &&
//        col("purchase_amount")<=2000,"Medium").otherwise("Small").alias(("Purchase_category")))
////    val df2 = df.filter(col("purchase_date").contains("2024-01"))df2.show()
//    val window = Window.partitionBy("Purchase_category")
//    val df3 = df1.withColumn("Sum",sum("purchase_amount").over(window))
//    val df4 = df3.withColumn("Avg",avg("purchase_amount").over(window))
//    val df5 = df4.withColumn("Max",max("purchase_amount").over(window))
//    val df6 = df5.withColumn("Min",min("purchase_amount").over(window))
//    df6.show()

    //Question_8(Set_B)
//    val df = List(
//      (1, "North-West", "12000", "2024-01-10"),
//      (2, "South-East", "6000", "2024-01-15"),
//      (3, "East-Central", "4000", "2024-02-20"),
//      (4, "West", "15000", "2024-02-25"),
//      (5, "North-East", "3000", "2024-03-05"),
//      (6, "South-West", "7000", "2024-03-12")
//    ).toDF("sales_id", "region", "sales_amount", "sales_date")
//    val df1 = df.select(col("sales_id"),col("region"),col("sales_amount"),col("sales_date"),
//      when(col("sales_amount")>10000,"Excellent").when(col("sales_amount")>=5000 && col("sales_amount")<=10000,"Good")
//        .otherwise("Average").alias(("Sales_Performance")))
// //   val df2 = df.filter(col("region").endsWith("West"))df2.show()
//    val window = Window.partitionBy("Sales_Performance")
//    val df3 = df1.withColumn("Sum",sum("sales_amount").over(window))
//    val df4 = df3.withColumn("Avg",avg("sales_amount").over(window))
//    val df5 = df4.withColumn("Max",max("sales_amount").over(window))
//    val df6 = df5.withColumn("Min",min("sales_amount").over(window))
//    df6.show()

    //Question_7(Set_B)
//    val df = List(
//      (1, "Smartphone", 4, "2024-01-15"),
//      (2, "Speaker", 3, "2024-01-20"),
//      (3, "Smartwatch", 5, "2024-02-15"),
//      (4, "Screen", 2, "2024-02-20"),
//      (5, "Speakers", 4, "2024-03-05"),
//      (6, "Soundbar", 3, "2024-03-1")
//    ).toDF("review_id", "product_name", "rating", "review_date")
//    val df1 = df.select(col("review_id"),col("product_name"),col("rating"),col("review_date"),
//      when(col("rating")>=4,"High").when(col("rating")<=3 && col("rating")>4,"Medium")
//        .otherwise("Low").alias(("Rating_category")))
////    val df2 = df.filter(col("product_name").startsWith("S"))df2.show()
//    val window = Window.partitionBy("Rating_category")
//    val df3 = df1.withColumn("Count",count("rating").over(window))
//    val df4 = df1.withColumn("Avg",avg("rating").over(window))
//    df4.show()

    //Question_6(Set_B)
//    var df = List(
//      (1, "2024-01-10", 8, "Good performance"),
//      (2, "2024-01-15", 9, "Excellent work!"),
//      (3, "2024-02-20", 6, "Needs improvement"),
//      (4, "2024-02-25", 7, "Good effort"),
//      (5, "2024-03-05", 10, "Outstanding!"),
//      (6, "2024-03-12", 5, "Needs improvement")
//    ).toDF("Employee_id", "Review_date", "Performance_score", "Review_text")
//    val df1 = df.select(col("Employee_id"),col("Review_date"),col("Performance_score"),col("Review_text"),
//    when(col("Performance_score")>=9,"Excellent").when(col("Performance_score")>=7 && col("Performance_score")<9,"Good")
//      .otherwise("NeedsImprovement").alias(("Performance_category")))
    //val df2 = df.filter(col("Review_text").contains("excellent"))
//    val window = Window.partitionBy(month(col("Review_date")))
//    val df3 = df.withColumn("Avg",avg("Performance_score").over(window))
//    df3.show()



    //Question_5(Set_B)
//    var df = List(
//      (1, 1, "1200", "2024-01-15"),
//      (2, 2, "600", "2024-01-20"),
//      (3, 3, "300", "2024-02-15"),
//      (4, 4, "1500", "2024-02-20"),
//      (5, 5, "200", "2024-03-05"),
//      (6, 6, "900", "2024-03-12")
//    ).toDF("Transaction_id", "Customer_id", "Transaction_amount", "Transaction_date")
//    var df1 = df.select(col("Transaction_id"),col("Customer_id"),col("Transaction_amount"),col("Transaction_date"),
//      when(col("Transaction_amount")>1000,"High").when(col("Transaction_amount")>=500 && col("Transaction_amount")<=1000,"Medium")
//        .otherwise("Low").alias(("Transaction_category")))
////    var df2 = df.withColumn("Transaction_year",year(col("Transaction_date")))
////    var df3 = df2.filter(col("Transaction_year").contains("2024"))
//    var window = Window.partitionBy("Transaction_category")
//    var df4 = df1.withColumn("Sum",sum("Transaction_amount").over(window))
//    var df5 = df4.withColumn("Avg",avg("Transaction_amount").over(window))
//    var df6 = df5.withColumn("Max",max("Transaction_amount").over(window))
//    var df7 = df6.withColumn("Min",min("Transaction_amount").over(window))
//    df7.show()

    //Question_4(Set_B)
//    var df = List(
//      (1, "Pro Widget", 30, "2024-01-10"),
//      (2, "Pro Device", 120, "2024-01-15"),
//      (3, "Standard", 200, "2024-01-20"),
//      (4, "Pro Gadget", 40, "2024-02-01"),
//      (5, "Standard", 60, "2024-02-10")
//    ).toDF( "Product_id", "Product_name", "Stock","Last_restocked")
//    var df1 = df.select(col("Product_id"),col("Product_name"),col("Stock"),col("last_restocked"),
//      when(col("Stock")<50,"Low").when(col("Stock")>=50 && col("Stock")<=150,"Medium").
//        otherwise("High").alias(("Stock_status")))
// //   var df2 = df.filter(col("Product_name").contains("Pro"))df2.show
//    var window = Window.partitionBy("Stock_status")
//    var df3 = df1.withColumn("Sum",sum("Stock").over(window))
//    var df4 = df3.withColumn("Avg",avg("Stock").over(window))
//    var df5 = df4.withColumn("Max",max("Stock").over(window))
//    var df6 = df5.withColumn("Min",min("Stock").over(window))
//    df6.show()

    //Question_3(Set_B)
//    val df = List(
//      (1, "2024-01-10", 9, "Sales"),
//      (2, "2024-01-11", 7, "Support"),
//      (3, "2024-01-12", 8, "Sales"),
//      (4, "2024-01-13", 10, "Marketing"),
//      (5, "2024-01-14", 5, "Sales"),
//      (6, "2024-01-15", 6, "Support")
//    ).toDF("Employee_id", "Work_date", "Hours_worked", "Department")
  //  var df1 = df.select(col("Employee_id"),col("Work_date"),col("Hours_worked"),col("Department"),
    // when(col("Hours_worked")>8,"Overtime").otherwise("Regular").alias(("Hours_category")))df1.show()
 //   var df2 = df.filter(col("Department").startsWith("S"))df2.show()
//    var window = Window.partitionBy("Department")
//    var df3 = df.withColumn("Sum",sum("Hours_worked").over(window))
//   var df4 = df3.withColumn("Avg",avg("Hours_worked").over(window))
//    var df5 = df4.withColumn("Max",max("Hours_worked").over(window))
//    var df6 = df5.withColumn("Min",min("Hours_worked").over(window))
//    df6.show()

    //Question_2(Set_B)
//    val df = Seq(
//      (1, "Widget", 700, "2024-01-15"),
//      (2, "Gadget", 150, "2024-01-20"),
//      (3, "Widget", 350, "2024-02-15"),
//      (4, "Device", 600, "2024-02-20"),
//      (5, "Widget", 100, "2024-03-05"),
//      (6, "Gadget", 500, "2024-03-12")
//    ).toDF("Sale_id", "Product_name", "Sale_amount", "Sale_date")

//    var df1 = df.select(col("Sale_id"),col("Product_name"),col("Sale_amount"),col("Sale_date"),
//      when(col("Sale_amount")>500,"High").when(col("Sale_amount")>=200 && col("Sale_amount")<=500,"Medium")
//        .otherwise("Low").alias(("Sale_category")))df1.show()
//    var df2 = df.filter(col("Product_name").endsWith("t"))df2.show()
//    var window = Window.partitionBy(month(col("Sale_date")))
//  var df3 = df.withColumn("Sum",sum("Sale_amount").over(window))
//    var df4 = df3.withColumn("Avg",avg("Sale_amount").over(window))
//    var df5 = df4.withColumn("Max",max("Sale_amount").over(window))
//    var df6 = df5.withColumn("Min",min("Sale_amount").over(window))
//    df6.show()


    //Question_1(Set-B)
//    val df = Seq(
//      (1, "2024-01-10", 4, "Great service!"),
//      (2, "2024-01-15", 5, "Excellent!"),
//      (3, "2024-02-20", 2, "Poor experience"),
//      (4, "2024-02-25", 3, "Good value"),
//      (5, "2024-03-05", 4, "Great quality"),
//      (6, "2024-03-12", 1, "Bad service")
//    ).toDF("Customer_id", "Feedback_date", "Rating", "Feedback_text")
//    val df1 = df.select(col("Customer_id"),col("Feedback_date"),col("Rating"),col("Feedback_text"),
//      when(col("Rating")>=5,"Excellent").when(col("Rating")>=3 && col("Rating")<5,"Good")
//        .otherwise("Poor").alias("Rating_category"))
   // val df2 = df.filter(col("Feedback_text").startsWith("Great"))df2.show()
//    val window = Window.partitionBy(month(col("Feedback_date")))
//    val df3 = df.withColumn("Avg",avg("Rating").over(window))
//    df3.show()


    //Question_5
//    val df = Seq(
//      (1, "2023-12-01", 1200, "Credit"),
//      (2, "2023-11-15", 600, "Debit"),
//      (3, "2023-12-20", 300, "Credit"),
//      (4, "2023-10-10", 1500, "Debit"),
//      (5, "2023-12-30", 250, "Credit"),
//      (6, "2023-09-25", 700, "Debit")
//    ).toDF("Transaction_id", "Transaction_date", "Amount", "Transaction_type")
//     val df1 = df.select(col("Transaction_id"),col("Transaction_date"),col("Amount"),col("Transaction_type"),
//       when(col("Amount")>1000,"High").when(col("Amount")<=500 && col("Amount")<=1000,"Medium")
//         .otherwise("Low").alias(("Amount_category")))
    //val df2 = df.withColumn("Transaction_month",month(col("Transaction_date")))
    //val df3 = df.filter(col("Transaction_date").startsWith("2023-12-01").alias("Transaction_month"))
//    val window = Window.partitionBy("Transaction_type")
    //val df4 = df.withColumn("Sum",sum("Amount").over(window))
    //val df5 = df.withColumn("Avg",avg("Amount").over(window))
    //val df6 =  df.withColumn("Max",max("Amount").over(window))
//    val df7 =  df.withColumn("Min",min("Amount").over(window))
//    df7.show()

    //Question_4
//    val df = List(
//      (1, "The Matrix", 9, "136"),
//      (2, "Inception", 8, "148"),
//      (3, "The Godfather", 9, "175"),
//      (4, "Toy Story", 7, "81"),
//      (5, "The Shawshank Redemption", 10, "142"),
//      (6, "The Silence of the Lambs", 8, "118")
//    ).toDF("Movie_id", "Movie_name", "Rating", "Duration_minutes")
    //val df1 = df.select(col("Movie_id"),col("Movie_name"),col("Rating"),col("Duration_minutes"),
//      when(col("Rating")>=8,"Excellent").when(col("Rating")<=6 && col("Rating")<8, "Good").
//        otherwise("Average").alias(("Rating_category")),
//    when(col("Duration_minutes")>150,"Long").when(col("Duration_minutes")<=90 &&
//    col("Duration_minutes")<=150, "Medium").otherwise("Short").alias(("Duration_category")))
//    df1.show()
    //val df2 = df.filter(col("Movie_name").startsWith("T"))
//    val df3= df.filter(col("Movie_name").endsWith("e"))
//    df3.show()
   // val window = Window.partitionBy("Rating")
    //val df4 = df.withColumn("Rating_Category",sum(col("Duration_minutes")).over(window))
   // val df5 =  df.withColumn("Rating_Category",avg(col("Duration_minutes")).over(window))
    //val df6 = df.withColumn("Rating_Category",max(col("Duration_minutes")).over(window))
//    val df7 = df.withColumn("Rating_Category",min(col("Duration_minutes")).over(window))
//    df7.show()

    //Question_3
//    val df = List(
//        (1, "John", 28, "60000"),
//        (2, "John", 32, "75000"),
//        (3, "Mike", 45, "120000"),
//        (4, "Alice", 55, "90000"),
//        (5, "Steve", 62, "110000"),
//        (6, "Claire",40, "4000")
//    ).toDF("Employee_id", "Name", "Age", "Salary")
  //  val df1 = df.select(col("Employee_id"),col("Name"),col("Age"),col("Salary"),
//      when(col("Age")<30,"Young").
//        when(col("Age")>=30 && col("Age")<=50,"Mid").
//        when(col("Age")>50,"Senior").alias(("Column_age")),
//      when(col("Salary")>100000,"High").when(col("Salary")<=50000 && col("Salary")<=100000,"Medium").
//        otherwise("Low").alias(("Salary_range")))
//    df1.show()
    //val df2 = df.filter(col("Name").startsWith("J"))
    //val df3 = df.filter(col("Name").endsWith("e"))
     //df3.show()
    //val window = Window.partitionBy("Name")
    //val df4 = df.withColumn("Age_Group",sum(col("Salary")).over(window))
//    val df5 = df.withColumn("Age_Group",avg(col("Salary")).over(window))
//    df5.show()
//    val df6 = df.withColumn("Age_Group",min(col("Salary")).over(window))
//    df6.show()
//    val df7 = df.withColumn("Age_Group",max(col("Salary")).over(window))
//    df7.show()

    //Question_2
//    val df = List(
//      (1, "Smartphone", 700, "Electronics"),
//      (2, "TV", 1200, "Electronics"),
//      (3, "Shoes", 150, "Apparel"),
//      (4, "Socks", 25, "Apparel"),
//      (5, "Laptop", 800, "Electronics"),
//      (6, "Jacket", 200, "Apparel")
//    ).toDF("Product_id","Product_name","Price","Category")
//    val df1 = df.select(col("Product_id"),col("Product_name"),col("Price"),col("Category"),
//      when(col("Price")>500,"Expensive").
//      when(col("Price")>=200 && col("Price")<=500, "Moderate").
//        otherwise("Cheap").alias(("Price_Category")))
//    val df2 = df.filter(col("Product_name").startsWith("S"))
//    val df3 = df.filter(col("Product_name").endsWith("s"))
   // val window = Window.partitionBy("Category")
//    val df4 = df.withColumn("Total_Price",sum(col("Price")).over(window))
//    val df5 = df.withColumn("Total_Price",avg(col("Price")).over(window))
    //val df6 = df.withColumn("Total_Price",min(col("Price")).over(window))
//    val df7 = df.withColumn("Total_Price",max(col("Price")).over(window))
//    df7.show()

    //Question_1
//    val df = List(
//      (1, "Alice", 92, "Math"),
//      (2, "Bob", 85, "Math"),
//      (3, "Carol", 77, "Science"),
//      (4, "Dave", 65, "Science"),
//      (5, "Eve", 50, "Math"),
//      (6, "Frank", 82, "Science")
//    ).toDF("Student_id","Name","Score","Subject")

   // val df1 = df.select(col("Student_id"),col("Name"),col("Score"),col("Subject"),
//    when(col("Score")>=90, "A").
//      when(col("Score")<=80 && col("Score")<90, "B").
//      when(col("Score")<=70 && col("Score")<80, "C").
//      when(col("Score")<=60 && col("Score")<70, "D").
//      when(col("Score")<60, "F").alias(("Grade")))
//    df1.show()
//     val window = Window.partitionBy("Subject")
//       .orderBy("Score")
    //val df2 = df.withColumn("Average_Subject", avg(col("Score")).over(window))
    //df2.show()
//    val df3 = df.withColumn("Min_score", min(col("Score")).over(window))
//    df3.show()
//       val df4 = df.withColumn("Max_score", max(col("Score")).over(window))
//    df4.show()
//    val df5 = df.withColumn("Count", count(col("Name")).over(window))
//    df5.show()
//    df.groupBy("Subject").agg(avg("Score")).show()


  }
}

