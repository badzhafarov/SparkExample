import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}


case class DataRow(employee_name: String, department: String, salary: Int)


object SparkExamples extends App{

  val spark = SparkSession
    .builder()
    .appName("SparkPipeline")
    .config("spark.master", "local[2]")
    .getOrCreate()

  import spark.implicits._ // only for jvm api

  spark.conf.set("spark.sql.adaptive.enabled", false)

  val SalaryData = Seq(DataRow("James", "Sales", 3000),
    DataRow("Michael", "Sales", 4600),
    DataRow("Robert", "Sales", 4100),
    DataRow("Maria", "Finance", 3000),
    DataRow("James", "Sales", 3000),
    DataRow("Scott", "Finance", 3300),
    DataRow("Jen", "Finance", 3900),
    DataRow("Jeff", "Marketing", 3000),
    DataRow("Kumar", "Marketing", 2000),
    DataRow("Saif", "Sales", 4100)
  ).toDF()

  SalaryData.show(10)

  val is_sales = col("department")===lit("Sales") // === for jvm api, for python ==
  val is_marketing = col("department")===lit("Marketing")

  val gt_1000 = col("salary") > 1000
  val gt_1500 = col("salary") > 1500

  val EXAMPLE_FLAG: Int = 1

  if (EXAMPLE_FLAG==1){

    val df = SalaryData.filter(!is_sales)

    df.filter(is_marketing && gt_1000).count()
    df.filter(is_marketing && gt_1500).count()
    df.filter(is_marketing && gt_1500).count()

  } else if (EXAMPLE_FLAG==2){

    val df = SalaryData.filter(!is_sales)

    df.filter(is_marketing && gt_1000).cache().count()
    df.filter(is_marketing && gt_1500).count()
    df.filter(is_marketing && gt_1500).count()

  }

  Thread.sleep(200000)

}
