
source("./R/eqatools.R")


########################################################################
#
# sample test 1a  knox - machine test (AuthMech=3) via RODBC
#
########################################################################
#
library(RODBC)
# twauslaenapp04.app.test.hcscint.net
# twauslmnapp01.app.test.hcscint.net

DSN <- sprintf("Driver=%s;HOST=%s;PORT=%s;UID=%s;PWD=%s;FastSQLPrepare=0;UseNativeQuery=0;HiveServerType=2;AuthMech=3;SSL=1;TwoWaySSL=0;ThriftTransport=2;HTTPPath=gateway/default/hive;ValidateServerCertificate=0;AllowSelfSignedServerCert=1;CAIssuedCertNamesMismatch=1;",
               "Hortonworks Hive ODBC Driver",
               "twauslaenapp04.app.test.hcscint.net",
               "8443",
               "u365042",
               getPass::getPass(msg=paste0(" Enter  Password"))
)

mycon <-  RODBC::odbcDriverConnect(DSN)
RODBC::odbcQuery(mycon,"use default")
RODBC::sqlQuery(mycon,"show tables")



########################################################################
#
# sample test 1b  knox - machine test (AuthMech=3) via RJDBC
#
########################################################################
#

library("rJava")
library("RJDBC")
hadoop.class.path = list.files(path=c('C:/Users/U365042/.dbeaver-drivers/drivers/hadoop'),pattern="jar", full.names=T);
drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver",classPath = hadoop.class.path,identifier.quote="`")
conn <- RJDBC::dbConnect(drv,"jdbc:hive2://twauslaenapp04.app.test.hcscint.net:8443/default;ssl=true;sslTrustStore=C:/pwauslaenapp04.jks;trustStorePassword=test123;transportMode=http;httpPath=gateway/default/hive;AuthMech=3;",
                         "u365042",
                         getPass::getPass(msg=" Enter  Password") )
RJDBC::dbGetQuery(conn,"show databases")
RJDBC::dbSendUpdate(conn,"use default")
RJDBC::dbGetQuery(conn,"show tables")






########################################################################
#
# sample test 2a - try kerberos
# use the same beeline -u "jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/;principal=hive/_HOST@ADHCSCTST.NET;transportMode=http;httpPath=cliservice"

########################################################################

joptions<-list(
  java.security.krb5.conf="C:/Windows/krb5.ini",
  java.security.krb5.realm="ADHCSCINT.NET",
  java.security.krb5.kdc="pwauswifdc16.adhcscint.net",
  javax.security.auth.useSubjectCredsOnly="false",
  sun.security.krb5.debug="true",
  java.security.auth.login.config="C:/temp/blah1.jaas",
  sun.security.jgss.debug="true"
)

joptionsLine<-paste0(lapply(seq_along(joptions), function(i) paste0("-D",names(joptions)[[i]],"=",joptions[[i]])), collapse = " ")


library("rJava")
library("RJDBC")
Sys.setenv(JAVA_TOOL_OPTIONS = joptionsLine)
# Sys.setenv(KRB5_CONFIG = "C:\\Windows\\krb5.ini")
# Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_161')

hadoop.class.path = list.files(path=c('C:/Users/U365042/.dbeaver-drivers/drivers/hadoop'),pattern="jar", full.names=T);
.jinit(classpath=hadoop.class.path,joptionsLine)



drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver",classPath = hadoop.class.path,identifier.quote="`")
#shell('"C:\\Program Files (x86)\\Java\\jre1.8.0_131\\bin\\kinit.exe" u365042@ADHCSCINT.NET')

#conn <- RJDBC::dbConnect(drv,"jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/default;principal=hive/_HOST@ADHCSCTST.NET;transportMode=http;httpPath=cliservice;AuthMech=1;" )
#conn <- RJDBC::dbConnect(drv,"jdbc:hive2://pwauslmnapp01.app.hcscint.net:10001/default;principal=hive/_HOST@ADHCSCINT.NET;transportMode=http;httpPath=cliservice;AuthMech=1;" )



RJDBC::dbGetQuery(conn,"show databases")
RJDBC::dbSendUpdate(conn,"use default")
RJDBC::dbGetQuery(conn,"show tables")



















########################################################################
#
# sample test 2  kerberos - cluster test (AuthMech=1)
#
########################################################################
# beeline -u "jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/;principal=hive/_HOST@ADHCSCTST.NET;transportMode=http;httpPath=cliservice"


DSN <- sprintf("Driver=%s;HOST=%s;PORT=%s;UID=%s;PWD=%s;FastSQLPrepare=0;TwoWaySSL=0;UseNativeQuery=0;HiveServerType=2;AuthMech=1;ServiceDiscoveryMode=0;ThriftTransport=2;HTTPPath=cliservice;KrbHostFQDN=_HOST;KrbServiceName=hive;KrbRealm=ADHCSCTST.NET;",
               "Hortonworks Hive ODBC Driver",
               "twauslmnapp01.app.test.hcscint.net",
               "10001",
               "u365042",
               getPass::getPass(msg=paste0(" Enter  Password"))
)
mycon <-  odbcDriverConnect(DSN)




rhive.connect(host="ntwauslmnapp01.app.test.hcscint.net:10000/default; principal=hive/_HOST@ADHCSCTST.NET;SSL=0;AuthMech=1;KrbHostFQDN=service.hortonworks.com;KrbServiceName=hive;KrbRealm=ADHCSCTST.NET", defaultFS="hdfs://node1.hortonworks.com/rhive", hiveServer2=TRUE,updateJar=FALSE)

########################################################################
#
# sample test 3  kerberos - cluster test (AuthMech=1)
#
########################################################################
# beeline -u "jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/;principal=hive/_HOST@ADHCSCTST.NET;transportMode=http;httpPath=cliservice"
devtools::install_local(path = 'C:/Users/U365042/Downloads/RHive-master/RHive-master/RHive')

library("DBI")
library("rJava")
library("RJDBC")

hadoop.class.path = list.files(path=c('C:/Users/U365042/.dbeaver-drivers/drivers/hadoop'),pattern="jar", full.names=T);
.jinit(classpath=hadoop.class.path,parameters= c("-Djavax.security.auth.useSubjectCredsOnly=false","-Djava.security.debug=gssloginconfig"))
drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver",classPath = hadoop.class.path,identifier.quote="`")
url.dbc =  paste0("jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/;principal=hive/_HOST@ADHCSCTST.NET;transportMode=http;httpPath=cliservice");
url.dbc = "jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/default;principal=hive/_HOST@ADHCSCTST.NET?transportMode=http;httpPath=cliservice;auth=kerberos;kerberosAuthType=fromSubject"
conn = RJDBC::dbConnect(drv, url.dbc);


conf = .jnew("org.apache.hadoop.conf.Configuration")
conf$set("hadoop.security.authentication", "Kerberos")
conf$set("debug", "true");

ugi = J("org.apache.hadoop.security.UserGroupInformation")
ugi$setConfiguration(conf)
ugi$loginUserFromSubject("U365042@ADHCSCINT.NET")
ugi$loginUserFromKeytab("U365042@ADHCSCINT.NET",  "C:\\Users\\U365042\\krb5.keytab")
library(RJDBC)
j=JDBC("org.apache.hive.jdbc.HiveDriver")
c=dbConnect(j, "jdbc:hive2://:/;principal=hive/@")
dbGetQuery(c, "show databases")

"jdbc:hive2://HiveHost:10000/default;" ||
  "principal=hive/localhost.localdomain@EXAMPLE.COM;" ||
  "auth=kerberos;kerberosAuthType=fromSubject";




Sys.setenv(JAVA_TOOL_OPTIONS="-Djavax.security.auth.useSubjectCredsOnly=false")
conn <- dbConnect(drv , "jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/;principal=hive/_HOST@ADHCSCTST.NET;transportMode=http;httpPath=cliservice")




# try this one
install.packages("httr")
install.packages("devtools")

library(httr)
set_config(
  use_proxy(url="bcproxy.hcscint.net", port=8080)
)
library(devtools)

kinit -f -c /tmp/hue_krb5_ccache


#jdbc:hive2://localhost:10001/default;transportMode=http;httpPath=cliservice



# -Dsun.security.krb5.debug=[ true | false ]
# -Dsun.security.jgss.debug=[ true | false ]
# -Djava.security.krb5.realm=[ example : aqua-internal.com ]
# -Djava.security.krb5.kdc=[ example : kdc.aqua-internal.com ]
# -Djava.security.krb5.conf=[ example: /etc/krb5.conf | c:\windows\krb5.ini ]
# -Djava.security.auth.login.config=[ example : /etc/jaas.conf | c:\windows\jaas.conf ]
# -Djavax.security.auth.useSubjectCredsOnly=[ true | false ]

library(RJDBC)
hadoop.class.path = list.files(path=c('C:/Users/U365042/.dbeaver-drivers/drivers/hadoop'),pattern="jar", full.names=T);
.jinit(classpath=hadoop.class.path,parameters= c(
  "-Djava.security.auth.login.config=C:\\TEMP\\blah1.jaas",
  "-Djavax.security.auth.useSubjectCredsOnly=false",
  "-Dsun.security.krb5.debug=false",
  "-Dsun.security.jgss.debug=false",
  "-Djava.security.krb5.realm=ADHCSCINT.NET",
  "-Djava.security.krb5.conf=c:\\windows\\krb5.ini",
  "-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext"
))
drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver",classPath = hadoop.class.path,identifier.quote="`")
conn = RJDBC::dbConnect(drv, "jdbc:hive2://pwauslmnapp03.app.hcscint.net:10001/default;auth=noSasl;transportMode=http;httpPath=cliservice;principal=hive/_HOST@ADHCSCINT.NET")
#conn <- dbConnect(drv, "jdbc:hive2://192.168.0.104:10000/default", "admin", "admin")

conn <- dbConnect(drv, "jdbc:hive2://192.168.0.104:10000/default", "admin", "admin")




Sys.setenv(KRB5_CONFIG="C:\\TEMP\\krb5.conf")


hadoop.class.path = list.files(path=c('C:/Users/U365042/.dbeaver-drivers/drivers/hadoop'),pattern="jar", full.names=T);
.jinit(classpath=hadoop.class.path,parameters= c(
  "-Djava.security.auth.login.config=C:\\TEMP\\blah1.jaas",
  "-Djavax.security.auth.useSubjectCredsOnly=false",
  "-Dsun.security.krb5.debug=false",
  "-Dsun.security.jgss.debug=false",
  "-Djava.security.krb5.realm=ADHCSCINT.NET",
  "-Djava.security.krb5.conf=c:/temp/krb5.conf",
  "-Djava.security.debug=gssloginconfig,configfile,configparser,logincontext"
))
drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver",classPath = hadoop.class.path,identifier.quote="`")
conn = RJDBC::dbConnect(drv, "jdbc:hive2://pwauslmnapp03.app.hcscint.net:10001/default;transportMode=http;httpPath=cliservice;principal=hive/_HOST@ADHCSCINT.NET")



shell('"c:\\Program Files\\Java\\jdk1.8.0_121\\bin\\kinit.exe"  -f -c "C:\\TEMP\\BLAH1.TMP"')


#vsystem("kinit username@ADS.IU.EDU -k -t username.keytab")
system('"c:\\Program Files\\java\\jdk1.8.0_121\\bin\\kinit.exe" u365042')

C:\Users\U365042\krb5cc_U365042



# The name of the database schema to use when a schema is not explicitly specified in a query.
Schema=default

# Set to 0 to when connecting directory to Hive Server 2 (No Service Discovery).
# Set to 1 to do Hive Server 2 service discovery using ZooKeeper.
# Note service discovery is not support when using Hive Server 1.
ServiceDiscoveryMode=0

# The namespace on ZooKeeper under which Hive Server 2 znodes are added. Required only when doing
# HS2 service discovery with ZooKeeper (ServiceDiscoveryMode=1).
ZKNamespace=

  # Set to 1 if you are connecting to Hive Server 1. Set to 2 if you are connecting to Hive Server 2.
  HiveServerType=2

# The authentication mechanism to use for the connection.
#   Set to 0 for No Authentication
#   Set to 1 for Kerberos
#   Set to 2 for User Name
#   Set to 3 for User Name and Password
# Note only No Authentication is supported when connecting to Hive Server 1.
AuthMech=3

# The Thrift transport to use for the connection.
#	Set to 0 for Binary
#	Set to 1 for SASL
#	Set to 2 for HTTP
# Note for Hive Server 1 only Binary can be used.
ThriftTransport=2

# When this option is enabled (1), the driver does not transform the queries emitted by an
# application, so the native query is used.
# When this option is disabled (0), the driver transforms the queries emitted by an application and
# converts them into an equivalent from in HiveQL.
UseNativeQuery=0

# Set the UID with the user name to use to access Hive when using AuthMech 2 to 8.
UID=novetta
PWD=novetta

# The following is settings used when using Kerberos authentication (AuthMech 1 and 10)

# The fully qualified host name part of the of the Hive Server 2 Kerberos service principal.
# For example if the service principal name of you Hive Server 2 is:
#   hive/myhs2.mydomain.com@EXAMPLE.COM
# Then set KrbHostFQDN to myhs2.mydomain.com
#KrbHostFQDN=[Hive Server 2 Host FQDN]

# The service name part of the of the Hive Server 2 Kerberos service principal.
# For example if the service principal name of you Hive Server 2 is:
#   hive/myhs2.mydomain.com@EXAMPLE.COM
# Then set KrbServiceName to hive
#KrbServiceName=[Hive Server 2 Kerberos service name]

# The realm part of the of the Hive Server 2 Kerberos service principal.
# For example if the service principal name of you Hive Server 2 is:
#   hive/myhs2.mydomain.com@EXAMPLE.COM
# Then set KrbRealm to EXAMPLE.COM
#KrbRealm=[Hive Server 2 Kerberos realm]

# Set to 1 to enable SSL. Set to 0 to disable.
SSL=0

# Set to 1 to enable two-way SSL. Set to 0 to disable. You must enable SSL in order to
# use two-way SSL.
TwoWaySSL=0

# The file containing the client certificate in PEM format. This is required when using two-way SSL.
ClientCert=

  # The client private key. This is used for two-way SSL authentication.
  ClientPrivateKey=

  # The password for the client private key. Password is only required for password protected
  # client private key.
  ClientPrivateKeyPassword=



  KRB5_CONFIG


;Host=hs2_host;Port=hs2_port;
HiveServerType=2;AuthMech=1;ThriftTransport=SASL;Schema=Hive_
database;

hive/_HOST@ADHCSCTST.NET





Driver=Hortonworks Hive ODBC Driver;Host=Azure_HDInsight_Service_
host;Port=443;HiveServerType=2;AuthMech=6;SSL=1;Schema=Hive_database;UID=user_
name;PWD=password;ThriftTransport=HTTP;HTTPPath=hs2_HTTP_path

1
trustStorePassword=<your-trust-store-password> ClientCert=/home/mpetronic/keystore/keystore.jks




"jdbc:hive2://twauslmnapp01.app.test.hcscint.net:10001/;principal=hive/_HOST@ADHCSCTST.NET;transportMode=http;httpPath=cliservice"

#  prod


DSN<-"
Driver=Hortonworks Hive ODBC Driver;
Host=twauslmnapp01.app.test.hcscint.net;Port=10001;
HiveServerType=2;AuthMech=1;ThriftTransport=SASL;KrbRealm=hive/_HOST;KrbHostFQDN=ADHCSCTST.NET;KrbServiceName=cliservice

"



hive = hiveOdbc(host="twauslmnapp01.app.test.hcscint.net",user="u365042", port=10001 , driver.name = 'auto')
hive$connect()


install.packages("sparklyr")

sc <- spark_connect(master = "yarn-client",version = "1.6.0", config = list(default = list(spark.yarn.keytab = "C:\\Users\\U365042\\krb5cc_U365042", spark.yarn.principal = "u365042@HCSCINT.COM")))


