
source("../R/eqatools.R")
OUTPUT_FOLDER<-"C:\\TEMP"


fastloadObject <-fastload(datafile = "C:\\20180319_measure_metadata.csv",
                          schema = "ENTPR_EQA",
                          file.sep = ",", reservedWords = c('submeasure_key'),reservedWordAppendSuffix = "_blah")

myTable<-fastloadObject$tableName

# write out our indiviudal fastload table scripts ..
fastloadScriptFile<-paste0(OUTPUT_FOLDER,'\\',myTable,'.fastload')
fileConn<-file(fastloadScriptFile)
writeLines(fastloadObject$fastLoadScript, fileConn)
close(fileConn)



# execute the fastload and populate data for our tmp table
flog.info(paste0("     fastload table [",myTable,"]"))
shellResult<-shell(paste0("fastload < \"", fastloadScriptFile,"\" >> \"",paste0(OUTPUT_FOLDER,'\\_',myTable,'.log'),"\""), wait = T)
