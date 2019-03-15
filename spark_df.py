from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from datetime import datetime
import calendar
import sys

if __name__ == "__main__":
   conf = SparkConf().setAppName("UBER_APP")
   #conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   uber_main_rdd = sc.textFile("uber.txt")
   uber_main_rdd_head = uber_main_rdd.map(lambda x:(x.split(","))).first()
   uber_main_rdd_wo_head = uber_main_rdd.filter(lambda x: "dispatching_base_number" not in x). \
                                   map(lambda x:(x.split(",")[0], \
                                   datetime.strptime(x.split(",")[1],'%m/%d/%Y').strftime('%A'), \
                                   int(x.split(",")[2]),int(x.split(",")[3])))
   sqlctx = SQLContext(sc)
   df1 = uber_main_rdd_wo_head.toDF(uber_main_rdd_head)
   df1.registerTempTable("uber_tab")
   results1 = sqlctx.sql("select a.date, a.tot_trips from (Select date, sum(trips) tot_trips from uber_tab group by date) a order by a.tot_trips desc")
   results1.show()
