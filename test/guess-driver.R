pattern1<-"Hortonworks"
pattern2<-"ODBC"
# auto specified, try to guess based on args supplied..
odbcDriversInstalled<-names(readRegistry(key = "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers"))
foundDrivers<-odbcDriversInstalled[grep(x = odbcDriversInstalled, pattern = paste0("^",pattern1,"$|",pattern1,"(.*)",pattern2,"(.*)"))]
if (length(foundDrivers)==0)
{
  stop("Cannot determine suitable driver in auto mode.  Driver list: ",paste(odbcDriversInstalled,collapse = ","))
}
print(paste0("[odbc driver] discovered ",foundDrivers[1]))
