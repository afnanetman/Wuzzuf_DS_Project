package project;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class project {
    public static void main(String[] args) {
        final SparkSession honda_session1 = SparkSession.builder().appName("honda_session1").master("local[3]").getOrCreate();
        honda_session1.sparkContext().setLogLevel("ERROR");
        DataFrameReader data=  honda_session1.read() ;
        data.option("header","true") ;
        Dataset<Row> data_readed= data.csv("src/main/resources/Wuzzuf_Jobs.csv") ;
        //1-display first 20 row of data
         data_readed.show();

        //2-display summary and structure of data
        data_readed.printSchema();
        data_readed.describe().show();

        //3-cleanning data
       System.out.println("Count before removing nulls"+data_readed.count()) ;
        //3-1 drop null
        data_readed=data_readed.na().drop();
       System.out.println("Count after removing nulls"+data_readed.count()) ;

        //3-2 drop duplicates
       data_readed=data_readed.distinct() ;
       System.out.println("count after removing duplicates"+data_readed.count());

        //4-count jobs for each company and sort them descending

     Dataset<Row> jobsCount = data_readed.groupBy("Company").count().orderBy(desc("count"));

       //5-create pie chart
        jobsCount.show();
        PieChart chart = new PieChartBuilder().width (800).height (600).title ("Jobs Chart").build ();
        List<Row> jobs = jobsCount.select("Company").collectAsList();
        List<Row> count = jobsCount.select("count").collectAsList();
        //chart.addSeries(jobsCount.take(1))
        for(int i=0;i<20;i++)
        {
            chart.addSeries(jobs.get(i).get(0).toString(),Integer.parseInt(count.get(i).get(0).toString()));
        }

        new SwingWrapper (chart).displayChart ();


        //6-most popular jobtitle
       Dataset<Row> jobTitles = data_readed.groupBy("Title").count().sort(desc("count")) ;
       jobTitles.show();

        //7-bar chart
        CategoryChart chart2 = new CategoryChartBuilder().width (1024).height (768).title ("Most Popular Job Titles").xAxisTitle ("Job Titles").yAxisTitle ("Count").build ();
        List<Row> titles = jobTitles.select("Title").collectAsList();
        List<String> titlesString = titles.stream().map(r->r.get(0).toString()).limit(25).collect(Collectors.toList());
        count = jobTitles.select("count").collectAsList();
        List <Integer> countInt = count.stream().map(r->Integer.parseInt(r.get(0).toString())).limit(25).collect(Collectors.toList());

        chart2.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        //chart2.getStyler ().setHasAnnotations (true);
        chart2.getStyler().setXAxisLabelRotation(90);
        chart2.addSeries ("Job Title Count", titlesString, countInt);

        // Show it
        new SwingWrapper (chart2).displayChart ();


        //8-most popular areas
       Dataset<Row> locations = data_readed.groupBy("Location").count().sort(desc("count"));
       locations.show();

        //9-bar chart
        CategoryChart chart3 = new CategoryChartBuilder().width (1024).height (768).title ("Most Popular Locations Titles").xAxisTitle ("Locations").yAxisTitle ("Count").build ();
        List<Row> location = locations.select("Location").collectAsList();
        List<String> locationString = location.stream().map(r->r.get(0).toString()).limit(25).collect(Collectors.toList());
        count = locations.select("count").collectAsList();
        countInt = count.stream().map(r->Integer.parseInt(r.get(0).toString())).limit(25).collect(Collectors.toList());
        chart2.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart3.getStyler().setXAxisLabelRotation(90);
        chart3.addSeries ("Location Count", locationString, countInt);
        // Show it
        new SwingWrapper (chart3).displayChart ();

        //10-

        Dataset<Row>skills = data_readed.withColumn("skill1", split(col("Skills"), ",").getItem(0)).withColumn("skill2", split(col("Skills"), ",").getItem(1))
               .withColumn("skill3", split(col("Skills"), ",").getItem(2)).withColumn("skill4", split(col("Skills"), ",").getItem(3))
               .withColumn("skill5", split(col("Skills"), ",").getItem(4)).withColumn("skill6", split(col("Skills"), ",").getItem(5))
               .withColumn("skill7", split(col("Skills"), ",").getItem(6));
        skills.select(
                        expr("stack(7,'skill1',skill1,'skill2',skill2,'skill3',skill3,'skill4',skill4,'skill5',skill5,'skill6',skill6,'skill7',skill7) as (index,skill)"))
                .groupBy("skill").count().orderBy(desc("count")).show();
    }
    }
