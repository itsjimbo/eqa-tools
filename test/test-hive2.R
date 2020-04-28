


packages_to_install<-setdiff(c("rJava", "Rcpp", "RJSONIO", "bitops", "digest",
                               "functional", "stringr", "plyr", "reshape2", "dplyr",
                               "R.methodsS3", "caTools", "Hmisc","httr","devtools","rjson","httr","gorder"), rownames(installed.packages()))
install.packages(packages_to_install)

library(httr)
set_config(
  use_proxy(url="bcproxy.hcscint.net", port=8080)
)
library(devtools)
# install from github
# eg: https://github.com/s-u/krb5
devtools::install_github("s-u/krb5")

devtools::install_github("RevolutionAnalytics/dplyrXdf")



install.packages('rhdfs','rhbase','rmr2','plyrmr')


install.packages('C:/R_INSTALL/plyrmr_0.6.0.tar.gz', repos=NULL, type='source')
install.packages('C:/R_INSTALL/ravro_1.0.4.tar.gz', repos=NULL, type='source')
install.packages('C:/R_INSTALL/rhbase_1.2.1.tar.gz', repos=NULL, type='source')
install.packages('C:/R_INSTALL/rhdfs_1.0.8.zip', repos=NULL, type='source')
install.packages('C:/R_INSTALL/rmr2_3.3.1.tar.gz', repos=NULL, type='source')

files<-sapply(list.files("C:\\R_INSTALL",full.names = T, pattern = "*.tar.gz"),function(f){
 return(f)
})



Sys.setenv("HADOOP_PREFIX"="C:/Users/U365042/Downloads/hadoop-2.7.6/hadoop-2.7.6")
Sys.setenv("HADOOP_CMD"="C:/Users/U365042/Downloads/hadoop-2.7.6/hadoop-2.7.6/bin/hadoop")
Sys.setenv("HADOOP_STREAMING"="C:/Users/U365042/Downloads/hadoop-2.7.6/hadoop-2.7.6/contrib/streaming/hadoop-streaming-1.1.2.jar")


library(rmr2)

## map function
map <- function(k,lines) {
  words.list <- strsplit(lines, '\\s')
  words <- unlist(words.list)
  return( keyval(words, 1) )
}

## reduce function
reduce <- function(word, counts) {
  keyval(word, sum(counts))
}

wordcount <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format="text",
            map=map, reduce=reduce)
}


## delete previous result if any
system("/Users/hadoop/hadoop-1.1.2/bin/hadoop fs -rmr wordcount/out")

## Submit job
hdfs.root <- 'wordcount'
hdfs.data <- file.path(hdfs.root, 'data')
hdfs.out <- file.path(hdfs.root, 'out')
out <- wordcount(hdfs.data, hdfs.out)

## Fetch results from HDFS
results <- from.dfs(out)

## check top 30 frequent words
results.df <- as.data.frame(results, stringsAsFactors=F)
colnames(results.df) <- c('word', 'count')
head(results.df[order(results.df$count, decreasing=T), ], 30)




Sys.setenv("HADOOP_PREFIX"="C:/Users/U365042/Downloads/hadoop-2.7.6/hadoop-2.7.6")
Sys.setenv("HADOOP_CMD"="C:/Users/U365042/Downloads/hadoop-2.7.6/hadoop-2.7.6/bin/hadoop")
Sys.setenv("HADOOP_STREAMING"="C:/Users/U365042/Downloads/hadoop-2.7.6/hadoop-2.7.6/contrib/streaming/hadoop-streaming-1.1.2.jar")


library(rmr2)
library(rJava)
library(rhdfs)
hdfs.init()
