# Machine Learning at Scale
Flight analytics and cancellation prediction with sparklyr and pyspark

This project is for the end-to-end ML at Scale workshop. It creates an API that can predict the likelihood of a flight being cancelled based on historic flight data. The original dataset comes from [Kaggle](https://www.kaggle.com/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018). The workshop shows both the pyspark and sparklyr implementations and covers:

+ Data Science and Exploration
+ ML Model Building and Optimisation
+ ML Model Training
+ ML Model Serving
+ Deploying an Application


## Setup Required

### All users

!chmod 777 cdsw-build.sh


### Python Users

`!pip3 install flask`


### R - users
While the ongoing dbplyr issue is waiting for a fix, run the following in a terminal to get it installed:

```
wget https://cran.r-project.org/src/contrib/dbplyr_1.4.3.tar.gz
tar xzvf dbplyr_1.4.3.tar.gz
echo 'importFrom(magrittr,"%>%")' >> dbplyr/NAMESPACE
Rscript -e 'install.packages("DBI")'
R CMD INSTALL dbplyr
```

`install.packages(c("sparklyr","psych","ggthemes","leaflet"))`

http://blog.cloudera.com/blog/2017/02/analyzing-us-flight-data-on-amazon-s3-with-sparklyr-and-apache-spark-2-0/
