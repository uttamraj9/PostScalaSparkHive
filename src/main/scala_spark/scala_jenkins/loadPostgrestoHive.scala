package scala_jenkins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object loadPostgrestoHive
{
      def main(args: Array[String]): Unit =
      {
      val spark: SparkSession = SparkSession.builder().master("local[*]").appName("MiniPrjScala").enableHiveSupport().getOrCreate()
      val df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "emp_info_scala").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()
      println(df.printSchema())
      println(df.show(10))
      println("Automated")


      //df_IT.write.mode("overwrite").saveAsTable("product.IT")
    }


}
