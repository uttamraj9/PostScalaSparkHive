package scala_jenkins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Loadfirst {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("MiniPrjScala").enableHiveSupport().getOrCreate()
    val df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "people").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()
    println(df.printSchema())
    println(df.show(10))
    println("Automated")

    // Define the calculation of age
    //val df_age = df.withColumn("DOB", to_date(col("DOB"), "M/d/yyyy")).withColumn("age", floor(datediff(current_date(), col("DOB")) / 365))

    df.show(10)

    // Define the increments based on departments and gender
   /* val department_increment_expr = when(col("dept") === "IT", 0.1)
      .when(col("dept") === "Marketing", 0.12)
      .when(col("dept") === "Purchasing", 0.15)
      .when(col("dept") === "Operations", 0.18)
      .when(col("dept") === "Finance", 0.2)
      .when(col("dept") === "Management", 0.25)
      .when(col("dept") === "Research and Development", 0.15)
      .when(col("dept") === "Sales", 0.18)
      .when(col("dept") === "Accounting", 0.15)
      .when(col("dept") === "Human Resources", 0.12)
      .otherwise(0)

    // Calculate the increment based on department and gender
    val increment_expr = when(col("gender") === "Female", department_increment_expr + 0.1).otherwise(department_increment_expr)

    // Calculate the incremented salary based on department and gender
    val df_increment = df_age.withColumn("increment", col("salary") * increment_expr).withColumn("new_salary", col("salary") + col("increment"))

    // Show the updated DataFrame
    df_increment.show(10)

    // Sort the DataFrame by ID
    val sorted_df = df_increment.orderBy("ID")
    sorted_df.show(10)
*/

    df.write.mode( "overwrite").saveAsTable("usukprjdb.testscalaspark")
    println("In Hive")
  }

}

// mvn package
//spark-submit --master local --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar --class scala_jenkins.loadPostgrestoHive target/MiniPrjScala-1.0-SNAPSHOT.jar


