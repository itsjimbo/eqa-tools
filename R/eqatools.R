########################################################################
#
#  EQATOOLS R functions and helpers
#  Author: James_Banasiak@bcbsil.com
#  Description: Purpose is to provide some sort of common functions for all projects
#
########################################################################
#   ChangeLog
#########################################################################
#  2017.11.10 - Updated export() command for teradata - a two byte record
#               length was causing large datasets to have extra blank rows
#               This issue has been resolved by adding an extra sed command
#
#
#  2017.11.10 - used sed inline (sed -i) to preserve disk space for large files
#  2018.01.05 - updated to git repo, added Queue implementation to common, httr, flog (futile.logger) and custom logging for writing to both console and file..
#  2018.01.25 - migrate to using git and R::package
#
########################################################################


#
#' checkDependencies
#'
#' Checks dependicies on startup
#' @keywords dependencies require library
#' @export
#' @examples
#' # typical
#' rm(list=ls())
#' library('eqatools')
#' eqatools::checkDependencies()
#'
#' ########################################################################
#' #
#' #  Global Config (Get our globals.yml file and configure any special params here..)
#' #
#' globals <- config::get(file = "globals.yml", use_parent = TRUE)
#'
#' ########################################################################
#' #
#' # Logging setup
#' #
#' flog.threshold(INFO)
#' flog.appender(appender.file2(paste0("logs/myproject-",Sys.Date(),".log"), console=TRUE))
#' flog.info("Started...")

checkDependencies<-function(){
  options(java.parameters = "-Xmx4096m")
  GLOBAL_LIBS<-c('futile.logger','httr','getPass','RODBC','RJSONIO','data.table','readr','stringr','stringi','devtools','xlsx','RDCOMClient','htmlTable','pander','sqldf','DBI','tcltk','base64enc','digest','dplyr','tidyr','ggplot2','ggthemes','scales','ztable','gmodels','fst')

  # automatically install any missing packages
  if (length(setdiff(GLOBAL_LIBS, rownames(installed.packages()))) > 0) {
    install.packages(setdiff(GLOBAL_LIBS, rownames(installed.packages())))
  }
  invisible(lapply(GLOBAL_LIBS,require,character.only=TRUE))
  if (find_rtools()==F){
    warning("Certain features like td$export() will not function correctly with RTools support.")
    warning("Please download and install rTools https://cran.r-project.org/bin/windows/Rtools/")
  }
}

checkDependencies()


########################################################################
#
#  teraDataOdbc - RODBC teradata object interface
#  Author: James_Banasiak@bcbsil.com
#
#  Sample Useage:
#    td$query("SELECT 1,2")
#    td$query("CALL GLOBAL_TOOLS.TD_COMPRESS('ENTPR_EQA','TMP_SUBMEASURE_DIM2','Y',RC,MSG,OUTPUT_MESSAGES,ALTER_STMT)")
#
#' teraDataOdbc
#'
#' An R object interface to Teradata
#' This object can connect to teradata and perform tasks such as query and execute procedures.
#'
#' @param host the host to connect to defaults to PRODUCTION IP__PRODUCTION_HOST_IP
#' @param mode Session ANSI or TERADATA mode
#' @param driver.name Drive name to use, with auto mode it will guess
#' @param driver.pattern The driver pattern to search for defaults to Teradata
#' @param port Teradata default 1025
#' @keywords teradata
#' @export
#' @examples
#' # initialize
#' td = teraDataOdbc(user='u365042', driver.name ='auto', driver.pattern="Teradata")
#' td$connect()
#'
#' # simple
#' df<-td$query("SELECT 1,2")
#' print(df)
#'
#' # fastexport a table (must use double backslashes in directory)
#' # the chop parameter is used to chop off the first two bytes using sed as an outmod
#' td$exportTables(schema = "ENTPR_EQA", tables = c("TMP_MEASURE_DIM"), directory = "C:\\TEMP", chop=TRUE)
#'
#' # call stored proc
#' td$query("CALL GLOBAL_TOOLS.TD_COMPRESS('ENTPR_EQA','TMP_SUBMEASURE_DIM2','Y',RC,MSG,OUTPUT_MESSAGES,ALTER_STMT)")
#'
#' # more complex

teraDataOdbc <- function(host="IP__PRODUCTION_HOST_IP",user="", port=1025,
                         mode='TERADATA',
                         driver.name='auto',
                         driver.pattern='Teradata')
{
  CLAZZNAME<-'teraDataOdbc'
  ## Get the environment for this instance of the function.
  thisEnv <- environment()
  ## Globals
  user<-user
  host<-host
  port<-port
  mode<-mode
  connected<-F
  driver<-''
  driver.name <- driver.name
  connection <- ''
  pwd<-''
  me <- list(
    thisEnv = thisEnv,
    ## Define the accessors for the data fields.
    getEnv = function()
    {
      return(get("thisEnv",thisEnv))
    },
    getDriver = function(){
      return(get("driver",thisEnv))
    },
    getMode = function(){
      return(get("mode",thisEnv))
    },
    getDriverName = function(){
      myDriver<-get("driver.name",thisEnv)
      pattern1<-get("driver.pattern",thisEnv)
      pattern2<-"ODBC"
      if (myDriver=='auto')
      {
        # auto specified, try to guess based on args supplied..
        odbcDriversInstalled<-names(readRegistry(key = "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers"))
        foundDrivers<-odbcDriversInstalled[grep(x = odbcDriversInstalled, pattern = paste0("^",pattern1,"$|",pattern1,"(.*)",pattern2,"(.*)"))]
        if (length(foundDrivers)==0)
        {
          stop("Cannot determine suitable driver in auto mode.  Driver list: ",paste(odbcDriversInstalled,collapse = ","))
        }
        print(paste0("[odbc driver] discovered ",foundDrivers[1]))
        return(foundDrivers[1])

      }
      else
      {
        return(myDriver)
      }

    },

    getPort = function(){
      return(get("port",thisEnv))
    },
    isConnected = function(){
      return(isTRUE(get("connected",thisEnv)))
    },
    getConnection = function(){
      return(get("connection",thisEnv))
    },
    getUser = function(){
      return(get("user",thisEnv))
    },
    getHost = function(){
      return(get("host",thisEnv))
    },
    password = function(){
      p<-getPass::getPass(msg=paste0(CLAZZNAME, " Enter [",me$getUser(),"] Password"))
      assign("pwd",base64enc::base64encode(what = charToRaw(p)),envir = thisEnv)
      return(p)
    },
    getSchemaInfo = function(schema='ENTPR_EQA'){
      sql<-paste0("select  tablename AS TABLENAME,sum (currentperm)/1024**3 as GIGABYTES,sum (currentperm)/1024**2 as MEGABYTES,cast((max(CurrentPerm)-avg(CurrentPerm))/(1024**2) as decimal(18,2)) as WASTED_SPACE from dbc.allspaceV t1
                  where databasename='",schema,"' group by tablename order by tablename")

      if (isTRUE(me$isConnected())){
        return(me$query(sql))
      }else{
        stop("Not Connected!")
      }


    },
    exportTables= function(schema='ENTPR_EQA',tables=c(),directory='',FAST_LOAD_AUTH_PATH="F:/DATA/Settings/FastLoad_Login.txt",chop=F){

      # this function will use fexp and generate the following scripts:
      # 1) the sql file (show info) to re-create the table
      # 2) the fastload script file to re-load the table
      # 3) the fastexport script file
      # schema='ENTPR_EQA'; tables=c("PROVIDER_HIERARCHY","PROVIDER_HIERARCHY_DENORMALIZED_NM"); directory='c:\\tmp';

      YYYYMMDD<-format(Sys.Date(),"%Y%m%d")

      LOGTABLE<-paste0(schema,".","TMP_LOG_",format(Sys.Date(),"%Y%m%d%H%M%s"))

      if (directory==''){
        stop("Error: directory argument not supplied")
      }
      if (length(tables)==0){
        stop("Error:  tables argument not supplied")
      }
      if (system("fexp")!=0){
        stop("Error: Please install teradata utilities (fexp)")
      }

      # Create templates...
      fastexportTable<-"
      .BEGIN EXPORT SESSIONS 20;
      .EXPORT OUTFILE [OUTFILE]
      MODE RECORD format TEXT;
      [HEADER_QUERY] ;
      [DATA_QUERY];
      .END EXPORT;
      "

      fastexportTemplate<-"
      .LOGTABLE [LOGTABLE];
      .LOGON  [HOST]/[USER],[pwd];
      DATABASE [SCHEMA];
      [fastExportLines]
      .LOGOFF;"

      fastloadScriptTemplate="
      SESSIONS 10;
      TENACITY 5;

      SLEEP 5;
      ERRLIMIT 50;
      /**
      Dependancy: A .fastload file that contains the following information
      .logon dbhostid/user,pass;

      This is inside the FAST_LOAD_AUTH_PATH, on windows usually  C:/Users/USERNAME/Documents/

      **/

      .RUN [FAST_LOAD_AUTH_PATH];
      .SET RECORD VARTEXT '[CSV_FILE_SEPERATOR]' ;
      RECORD 2; /* this skips first record in the source file */
      DEFINE
      [CSV_DEFINES]


      FILE=[CSV_FILENAME];
      SHOW;
      BEGIN LOADING [TABLENAME] ERRORFILES [TABLENAME]_error1,[TABLENAME]_error2
      CHECKPOINT 10000;

      INSERT INTO [TABLENAME]
      VALUES
      (
      [INSERT_COLUMNS]
      );

      END LOADING;
      .LOGOFF;
      .QUIT;
      ";


      powerShellScript<-"
      $f1=$args[0]
      $f2=$args[1]
      $Encoding = [Text.Encoding]::GetEncoding(28591)
      #$Encoding = [system.Text.Encoding]::ASCII
      #$Encoding = [system.Text.Encoding]::Unicode


      $if = new-object System.IO.StreamReader  -ArgumentList  $f1,$Encoding
      $of = new-object System.IO.StreamWriter $f2

      [Char[]]$buffer = new-object char[] 5000000
      [int]$lastByteWritten=0
      [int]$bytesRead = $if.ReadBlock($buffer, 0, $buffer.Length)


      while ($bytesRead -gt 0)
      {
      [int]$bytesToWrite = $bytesRead
      [string]$Chunk = New-Object string($buffer, 0, $bytesToWrite)
      $Chunk = $Chunk -replace \"`n`n\",\"`n1\"
      $Chunk = $Chunk -replace \"`r`n\",\"`n\"
      $Chunk = $Chunk -replace \"`0\",\"\"

      $of.Write( $Chunk )
      $lastByteWritten=$Chunk[$bytesToWrite]
      [int]$bytesRead = $if.ReadBlock($buffer, 0, $buffer.Length)

      }

      $if.Close()
      $if.Dispose()

      $of.Close()
      $of.Dispose()
      "

      fastExportLines<-sapply(tables,function(table){
        # table<-'PROVIDER_HIERARCHY'
        fullTableName<-paste(schema,table,sep='.')
        SQLOUTFILE<-file.path(gsub(x = directory,replacement = '\\\\\\\\',pattern = '\\\\'),paste0(YYYYMMDD,'-',schema,'_',table,'.sql'),fsep = '\\\\')
        # write the create table sql
        tblInfo<-sqlQuery(me$getConnection(),paste0("SHOW TABLE ",fullTableName));
        fileConn<-file(SQLOUTFILE)
        writeLines( text =as.character(tblInfo[[1]]) , fileConn)
        close(fileConn)

        # build the header row tab-delimited
        colInfo<-sqlQuery(me$getConnection(),paste0("SELECT * FROM ",fullTableName, " SAMPLE 1"));
        hdr<-paste0("SELECT ", paste0("'",names(colInfo),"'",collapse = "|| '09'XC ||"))
        # wrap the header query in another sub query becasue teradata does not like it
        hdr<-paste0("SELECT * FROM (",hdr, " AS HEADER) AS HEADERROW")

        # build the select data statment
        dat<-paste0("SELECT ",paste0(" coalesce(trim(",names(colInfo),"),'') ",collapse = "|| '09'XC ||"), " FROM ", fullTableName)

        DAT_FILENAME<-paste0(YYYYMMDD,'-',schema,'_',table,'.dat')
        OUTFILE<-file.path(gsub(x = directory,replacement = '\\\\\\\\\\\\\\\\',pattern = '\\\\'),DAT_FILENAME,fsep = '\\\\\\\\')

        # build the fastload import file from template
        SQL_INSERT_COLUMNS=sapply(names(colInfo),function(x){
          paste0(':',x)
        })
        #CSV_READ_COLUMNS<-regmatches(tblInfo$`Request Text`, gregexpr("(?=\\().*", tblInfo$`Request Text`, perl=T))[[1]]
        CSV_READ_COLUMNS=sapply(names(colInfo),function(x){
          paste0(x," (VARCHAR(255))")
        })

        fastloadScript<-fromTemplateString(fastloadScriptTemplate,
                                      list(
                                        '[FAST_LOAD_AUTH_PATH]'= FAST_LOAD_AUTH_PATH,
                                        '[CSV_DEFINES]'=paste(CSV_READ_COLUMNS, collapse = ',\n'),
                                        '[CSV_FILENAME]'=OUTFILE,
                                        '[CSV_FILE_SEPERATOR]'='\t',
                                        '[TABLENAME]'=fullTableName,
                                        '[INSERT_COLUMNS]' =paste(SQL_INSERT_COLUMNS, collapse = ',\n')
                                      ))
        # write script to a file
        fastload_script_abs_path<-file.path(gsub(x = directory,replacement = '\\\\\\\\',pattern = '\\\\'),paste0(YYYYMMDD,'-',schema,'_',table,'.fastload'),fsep = '\\\\')
        fileConn<-file(fastload_script_abs_path)
        writeLines( text =fastloadScript , fileConn)
        close(fileConn)


        return(fromTemplateString(fastexportTable,
                             list(
                               '[OUTFILE]'=OUTFILE,
                               '[HEADER_QUERY]'=hdr,
                               '[DATA_QUERY]'=dat
                             )))
      })

      fastExport <-fromTemplateString(fastexportTemplate,
                                 list(
                                   '[HOST]'=get("host",thisEnv),
                                   '[USER]'=get("user",thisEnv),
                                   '[SCHEMA]'=schema,
                                   '[pwd]'=rawToChar(base64enc::base64decode(what = get("pwd",envir = thisEnv))),
                                   '[LOGTABLE]'=LOGTABLE,
                                   '[fastExportLines]' = paste(fastExportLines,collapse = '\n')
                                 ))



      fastExportFile<-tempfile(fileext = '.fastexport')
      fileConn<-file(fastExportFile)
      writeLines(fastExport, fileConn)
      close(fileConn)
      # Execute the fast export script
      shell(paste0("fexp < ",fastExportFile))
      unlink(fastExportFile)

      # Remove the 2 byte row size for each file
      allFilesCreated<-sapply(tables,function(table){
        DAT_FILENAME<-paste0(YYYYMMDD,'-',schema,'_',table,'.dat')
        ABS_FILENAME<- paste(directory,DAT_FILENAME,sep = "\\")

        # ABS_FILENAME<-'c:\\tmp\\20171110-ENTPR_EQA_PRSP_LGCL_201706_CLM_V006.dat'

        if (file.exists(ABS_FILENAME)){

          powershell<-tempfile(fileext = '.ps1')
          fileConn<-file(powershell)
          writeLines(powerShellScript, fileConn)
          close(fileConn)


          if (chop)
          {
            print(paste0("[powershell/sed] Removing 2 byte record length ",ABS_FILENAME))
            shell(paste0("powershell -file  ",powershell, " \"",ABS_FILENAME,"\" \"",ABS_FILENAME,"2\""))
            shell(paste0("sed -i 's/^..//'  \"",ABS_FILENAME,"2\""))
            shell(paste0("sed -i \"$a\\\\\"  \"",ABS_FILENAME,"2\""))
            # switch the file .dat2 back to .dat (deletes the .dat and renmaes .dat2 to .dat)
            unlink(ABS_FILENAME)
            file.rename(from = paste0(ABS_FILENAME,'2'), to = ABS_FILENAME)
          }


          # compress the files
          SQLOUTFILE<-file.path(gsub(x = directory,replacement = '\\\\\\\\',pattern = '\\\\'),paste0(YYYYMMDD,'-',schema,'_',table,'.sql'),fsep = '\\\\')
          fastload_script_abs_path<-file.path(gsub(x = directory,replacement = '\\\\\\\\',pattern = '\\\\'),paste0(YYYYMMDD,'-',schema,'_',table,'.fastload'),fsep = '\\\\')
          ZIPFILENAME<-paste(directory,paste0(YYYYMMDD,'-',schema,'_',table,'.zip'),sep = "\\")

          zip(zipfile = ZIPFILENAME,files = list(ABS_FILENAME,SQLOUTFILE,fastload_script_abs_path), flags="-j")

          # remove the original files (they are part of the zip archive now..)
          unlink(SQLOUTFILE)
          unlink(fastload_script_abs_path)
          unlink(ABS_FILENAME)

          return(ZIPFILENAME)
        }
        else
        {
          print(paste0("File does not exist! ",ABS_FILENAME))
        }
      })

      return(allFilesCreated)
    },
    exportTablebyChunks = function(schema='ENTPR_EQA',tableName='',filename='', chunkSize=1e6, maxRecords=0,compress=0){
      require(fst)

      if (filename==''){
        stop("Error: filename argument not supplied")
      }
      if (tableName==''){
        stop("Error:  tableName argument not supplied")
      }
      # get the total from the table
      # schema='ENTPR_EQA'; tableName='IV_CL_T'; filename='c:\\tmp.fst';maxRecords=100;chunkSize=10;

      fullTableName<-paste(schema,tableName,sep='.')
      df1<-me$query(paste0("select count(*) AS TOTAL from ",fullTableName))
      maxRecords<-ifelse(maxRecords==0,df1$TOTAL,maxRecords)
      chunks <- seq(1,maxRecords,chunkSize)


      buildFilename<-function(filename,chunks,iteration){
        if(length(chunks)>1){
          return(file.path(dirname(filename), paste0(removeExtension(basename(filename)),'-',iteration,'.fst'), fsep = '\\'))
        }
        else
        {
          return(filename)
        }
      }
      allFiles<-c()
      for (i in (1:length(chunks))) {
        ptm<-proc.time()
        outputFilename<- buildFilename(filename,chunks,i)
        allFiles<-c(allFiles,outputFilename)
        if (i==1)
        {
          # first time through we call sqlFetch, consecutive request will be sqlFetchMore
          df<-RODBC::sqlFetch(channel = me$getConnection(), sqtable = fullTableName, max=chunkSize)
          write.fst(x = df,path =outputFilename, compress = compress )

        }
        else
        {
          df<-RODBC::sqlFetchMore(channel = me$getConnection(),   max=chunkSize)
          write.fst(x = df,path = outputFilename, compress = compress)
        }
        totalTime<-proc.time() - ptm
        print(paste0('Wrote chunk ',i,' of ',length(chunks),' in ',totalTime[3][[1]]))

      }
      return(allFiles)
    },
    reconnect = function(){
      connected<-T
      DSN <- sprintf("Driver=%s;DBCName=%s;UID=%s;PWD=%s",me$getDriverName(),me$getHost(),me$getUser(),rawToChar(base64enc::base64decode(what = get("pwd",envir = thisEnv))))
      mycon <- tryCatch(mycon <-  odbcDriverConnect(DSN), error = function(e) {
        connected<<-F
        print(e)
      });
      assign("connected",connected,thisEnv)
      if (isTRUE(me$isConnected())){
        assign("connection",mycon,thisEnv)
      }else{
        stop("Not Connected!")
      }

    },
    connect = function(host ='', user = '', port=''){
      if (host!='')
      {
        # set host
        assign("host",host,me)
      }
      if (user!='')
      {
        # set username
        assign("user",user,me)
      }
      if (port!='')
      {
        # set username
        assign("port",port,me)
      }
      connected<-T
      DSN <- sprintf("Driver=%s;DBCName=%s;UID=%s;PWD=%s",me$getDriverName(),me$getHost(),me$getUser(),me$password())
      mycon <- tryCatch(mycon <-  odbcDriverConnect(DSN), error = function(e) {
        connected<<-F
        print(e)
      });
      assign("connected",connected,thisEnv)
      if (isTRUE(me$isConnected())){
        assign("connection",mycon,thisEnv)
      }else{
        stop("Not Connected!")
      }

    },
    disconnect = function(){
      if (isTRUE(me$isConnected())){
        RODBC::odbcClose(me$getConnection())
        assign("connected",F,thisEnv)
      }else{
        stop("Not Connected!")
      }

    },
    query = function(sql){
      if (isTRUE(me$isConnected())){
        q=removeSQLComments(sql)
        firstWord<-tolower(stringr::str_extract(string =q, pattern = '([A-Za-z]+)'))
        isUpdateQuery<-F

        # reservedWords <-
        if (firstWord %in% c("macro", "new", "begin","end","create", "alter", "drop", "insert", "declare", "update","delete","call","collect"))
        {
          isUpdateQuery<-T
        }


        res<-sqlQuery(me$getConnection(),q);


        if (isUpdateQuery==F && class(res)=='character')
        {
          stop(paste0('Error loading query: ',res))
        }
        return(res)

      }else{
        stop("Not Connected!")
      }

    }

  )

  ## Define the value of the list within the current environment.
  assign('this',me,envir=thisEnv)

  ## Set the name for the class
  class(me) <- append(class(me),me$CLAZZNAME)
  return(me)
}



########################################################################
#
#  emailR - requires Outlook install (windows only) - uses COM Bridge
#  Author: James_Banasiak@bcbsil.com
#  uses htmlTable library to translate Dataframe into table for nice formatted  HTML emails
#  uses pander library to translate Dataframe into text
#  Full range of properties can be found here
#  https://msdn.microsoft.com/en-us/library/microsoft.office.interop.outlook.mailitem_properties.aspx
#
#
#' emailR
#'
#' Uses the COM bridge to talk to the outlook application (COMCreate)
#' A simple R object interface to the email client.  Can be used for attachments or including tables (dataframes) as html and plain text.
#' @param to the to email address
#' @param body the body of the email
#' @param cc the carbon copy
#' @param bcc the blind carbon copy
#' @keywords email
#' @export
#' @examples
#' df<-data.frame(First_Name=c("John","Jane"),Last_Name=c("Doe","Foo"))
#' write.csv(df, gzfile('C:/email-attachment-test.csv.gz'),row.names=FALSE,na = "")
#' testEmail = emailR(to = "James_Banasiak@bcbsil.com", subject = "Test Email", body = "Test Body")
#' testEmail$addHTMLTable(description = "Description", df = df)
#' testEmail$addAttachment('C:/email-attachment-test.csv.gz')
#' testEmail$save()
#' testEmail$display()
#' testEmail$send()
emailR <- function(to="",subject="",body="",cc="",bcc="")
{

  ## Get the environment for this instance of the function.
  thisEnv <- environment()
  ## Globals
  localEmail <- COMCreate("Outlook.Application")$CreateItem(0)
  localEmail[["To"]] = to
  localEmail[["CC"]] = cc
  localEmail[["BCC"]] = bcc
  localEmail[["subject"]] = subject
  localEmail[["HTMLBody"]] =body
  ## Create the list used to represent an
  ## object for this class
  me <- list(
    thisEnv = thisEnv,

    ## Define the accessors for the data fields.
    getEnv = function()
    {
      return(get("thisEnv",thisEnv))
    },
    addHTMLTable = function(description,df){
      me$addHTML( paste("",description,htmlTable(df), sep = "<BR/>"))
    },
    addAttachment = function(absolutePath){
      localEmail[["Attachments"]]$Add(absolutePath)
    },
    addPlainTextTable = function(description,df){
      panderOptions('table.split.table', Inf)
      me$addPlain( paste(localEmail[["body"]],description,"",pandoc.table.return(df), sep =  "\n"))
    },
    addHTML = function(html){
      localEmail[["HTMLBody"]] = paste(localEmail[["HTMLBody"]],html, sep = "")
    },
    addPlain = function(html){
      localEmail[["body"]] = paste(localEmail[["body"]],html, sep = "")
    },
    close=function(){
      localEmail$Close(2)
    },
    send=function(){
      localEmail$Send()
    },
    display=function(){
      localEmail$Display()
    },
    save=function(){
      localEmail$Save()
    },
    saveAs=function(localFile){
      localEmail$SaveAs(localFile)
    }

  )

  ## Define the value of the list within the current environment.
  assign('this',me,envir=thisEnv)

  ## Set the name for the class
  class(me) <- append(class(me),"emailR")
  return(me)
}





########################################################################
#

#
#' buildSQLInClause
#'
#' builds an IN criteria clause or a SELECT clause from a list
#' @param myList the list to build from
#' @param quoteChar a character to quote
#' @keywords select IN clause criteria builder
#' @export
#' @examples
#' buildSQLInClause(c(a,b,c),quoteChar="'")
buildSQLInClause<-function(myList,quoteChar=""){
  return(paste(sapply(myList,function(x){
    return(paste0(quoteChar,x,quoteChar))
  }), collapse = ","))
}





########################################################################
#
#  Reads a template file and performs variable/value replacement

#
#' fromTemplateFile
#'
#' reads a file and replaces expressions with the format [variable] with a specified value
#' @param file the template file to read
#' @param variableValueList a list of variables and values
#' @keywords template
#' @export
#' @examples
#' sql<-fromTemplateFile('example.sql'),list(
#'   '[VAR1]'=c("VAL1"),
#'   '[VAR2]'=paste(1,2,3,sep=','),
#'   '[VAR3]'="VAL3"
#' ))
#' print(sql)
#' debug.string(x = sql, fileext = '.sql')
#'
fromTemplateFile<-function(file,varlist){

  lines = readLines(file,warn = FALSE)
  lines = lines[which(lines!="")]
  lines = paste(lines,collapse ="\n")
  return(fromTemplateString(input = lines,varlist = varlist))

}


########################################################################
#
#  Reads a template file and performs variable/value replacement

#
#' fromTemplateString
#'
#' reads a string and replaces expressions with the format [variable] with a specified value
#' @param input the template file to read as a string
#' @param variableValueList a list of variables and values
#' @keywords template
#' @export
#' @examples
#' fromTemplateString("Hello [VAR1]  this is [VAR2] or test [VAR3]",list('[VAR1]'=c("VAL1"), '[VAR2]'=paste(1,2,3,sep=','), '[VAR3]'="VAL3"))
#'
fromTemplateString<-function(input,varlist){
  sql<-input
  sapply(seq_along(varlist), function(index, varvals, names) {
    var<-names[[index]]
    val <-varvals[index]
    #val<-paste(varvals[index],collapse = ",")
    #escape the variable brackets, which is a special character in regex
    var<-gsub('\\[','\\\\\\[',var)
    var<-gsub('\\]','\\\\\\]',var)
    # escape the value if it contains backslashes
    val<-gsub('\\\\','\\\\\\\\',val)
    #print(paste('replacing',var,val,sep=' '))
    sql<<-gsub(var, val,sql)
  },varvals=varlist,names=names(varlist))
  return(sql)
}




########################################################################
#
#' removeSQLComments
#'
#' Removes comments from sql
#'
#' @param x the string to remove commments from
#' @keywords comments
#' @export
#' @examples
#' removeSQLComments("
#' /***
#' multiline1
#' **/
#'              /* c++ style1 */
#' --single1
#' select 1 /*inside1*/,2
#'        /*
#' multiline2
#' */
#'
#'
#' --single2
#' /* c++ style2 */
#'
#'")
removeSQLComments<-function(x){
  #ret<-gsub("[\r\n]", " ", sub("/*[^\\*]*(?:\\*(?!/)[^*]*)*\\*/", "", x, perl=TRUE))
  ret<-x
  # remove single line comments
  ret<-gsub("(?://.*)|(?:--.*)", "", x,perl=TRUE)
  # remove multiline comments
  ret<-gsub("/\\*(?>(?:(?!\\*/|/\\*).)*)(?>(?:/\\*(?>(?:(?!\\*/|/\\*).)*)\\*/(?>(?:(?!\\*/|/\\*).)*))*).*?\\*/|--.*?\r?[\n]", "", ret, perl=TRUE)
  # remove single line inbetween
  ret<-sub("/*[^\\*]*(?:\\*(?!/)[^*]*)*\\*/", "", ret, perl=TRUE)
  # combine multiple \r\n into a single space (merge)
  ret<-gsub("[\r\n]", " ",ret,perl = TRUE)
  #ret<-gsub("/\\*(?>(?:(?!\\*/|/\\*).)*)(?>(?:/\\*(?>(?:(?!\\*/|/\\*).)*)\\*/(?>(?:(?!\\*/|/\\*).)*))*).*?\\*/|--.*?\r?[\n]", "", ret, perl=TRUE)
  #ret<-gsub("[\r\n]", " ",ret,perl = TRUE)
  # trim out
  return(trim.spaces(ret))
}


########################################################################
#
#  Sums individual digits
#  Exampe c(12,27) = c(3,9) where 1+2=3 and 2+7=9
########################################################################
#
#' sumdigits
#'
#'  Sums individual digits - used in NPI checksum
#'
#' @param x the list of digits
#' @keywords comments
#' @export
#' @examples
#' # where 1+2=3 and 2+7=9
#' sumdigits(c(12,27))
#' [1] 3 9
sumdigits <- function( x )
{
  if (  length(x)>0 &&  nchar(as.character(x))>0 )
  {
    sapply( x, function( xx )
      as.vector(sum(floor(xx / 10^(0:(nchar(xx) - 1))) %% 10))
    )
  }
  else
  {
    return(0)
  }

}

########################################################################
#
#' isValidNPI
#'
#' Computes a valid npi
#'
#' @param npi the npi
#' @keywords comments
#' @export
#' @examples
#' isValidNPI(1)
#' isValidNPI(1396860391)
########################################################################
#
# Check if NPI is valid
# NPI uses a variant of Luhn algorithm
# http://geekswithblogs.net/bosuch/archive/2012/01/16/validating-npi-national-provider-identifier-numbers-in-sql.aspx
isValidNPI <- function( npi ) {
  # split the digits
  npi <- as.character( npi )
  v <- as.numeric( unlist( strsplit(npi, "" ) ) )
  v <- rev( v )
  # double every second digit
  i2 <- seq( 2, length( v ), by= 2 )
  v[i2] <- v[i2] * 2
  v[ v > 9 ] <- as.vector(sumdigits( v[ v > 9 ] ))
  if( ( 24 + sum( v )) %% 10 == 0 ) return( TRUE )
  else return( FALSE )
}


########################################################################
#
#' debug.string
#'
#' Debugs a large string and opens the associated handler defined by the operating system
#'
#' @param x the string to open
#' @param fileext the fileextension for the os-lookup
#' @param startcmd the start command, for osx/linux use open
#'
#' @keywords debug large string
#' @export
#' @examples
#' debug.string(x = "SELECT 1,2", fileext = '.sql')
########################################################################
#
# Debugs a large string with associated file extension
# so that it will be opened in an editor mapped by the operating system
#
debug.string<-function(x,fileext,startcmd="start"){
  t<-tempfile(fileext = fileext)
  cat(x,file = t)
  shell(paste0(startcmd, " ",t))
}

########################################################################
#
#  Reads a multi seperator file
#  380Mb file of (1649167 rows) is approximately 140 seconds:
#
#   user  system elapsed
#   135.77    1.47  138.57
# Example (read 10 rows with custom header):

#
########################################################################
#
#' read.multisep
#'
#' reads a multi seperator file
#'
#' @param filename the file
#' @keywords comments
#' @export
#' @examples
#' f<-'PAL_PFIN_TAX_ID.txt'
#' df1<-read.multisep(filename=f,n=10L,sep = '|&~&|',headers=paste('column',c(1:15)))
#'
read.multisep <-function(filename, headers = NULL,sep = '|&~&|' , n = -1L, skipNul = TRUE) {
  #filename=f;n=10L;sep = '|&~&|';headers=paste('column',c(1:15)); skipNul = TRUE;
  ptm <- proc.time()
  con = file(filename, "rb")
  Lines <- readLines(con, n = n, skipNul = skipNul)
  Matrix <- do.call(rbind, strsplit(Lines, sep, fixed = TRUE))
  names<-headers
  if(is.null(headers)){
    names<-Matrix[1, ]
  }
  df <-structure(data.frame(Matrix[-1, ]), names = names)
  #df[] <- lapply(df,type.convert)                    ## automatically convert modes
  close(con)
  totalTime<-proc.time() - ptm
  print(totalTime)
  return(df)
}


########################################################################
#
#' checkDir
#'
#' Checks if a directory exists if it does not , it will create it
#'
#' @param directory the directory
#' @keywords directory
#' @export
#' @examples
#' checkDir('C:\\TMP')
checkDir=function(directory){  if (!dir.exists(directory)){ dir.create(directory) }}

########################################################################
#
#' my.max
#'
#' gets a max value
#'
#' @param x the vector
#' @keywords max
#' @export
#' @examples
#' my.max(c(1,2,3))
my.max <- function(x) ifelse( !all(is.na(x)), max(x, na.rm=T), NA)

########################################################################
#
#' removeExtension
#'
#'  removes an extension from a file
#'
#' @param file the filename
#' @keywords extension
#' @export
#' @examples
#' removeExtension("C:\\TEMP\\TESTFILE.123")
#' basename(removeExtension("C:\\TEMP\\TESTFILE.123"))
removeExtension<-function(file){
  return(sub(pattern = "(.*)\\..*$", replacement = "\\1", file))
}

########################################################################
#
#' fastload a file or dataset into teradata
#'
#' @param datafile the file to load, must have eol usually tab or comma seperated
#' @param sampleSize the sample to take from the file to determine column sizes
#' @param schema the schema to use when generating the fastload script
#' @param PRIMARY_KEY_COLUMNS list of columns specified as the primary key.  Default is "auto" which will auto-guess any column ending with PRIMARY_KEY_REGEX.  It will use the first column if nothing is specified which may slow indexing down or create wasted space
#' @param tableName the final tablename to use.  If omitted it will develop a table name based off the base name of the filename and prefix, eg a filename of myfilename123 will be TMP_myfilename123
#' @param tableNamePrefix table naming prefix to create if
#' @param override_column_sizes list of columns and grep expressions to use when determining columns sizes.  eg .*name.*="100" will force any field with the word name to be 100 bytes
#' @param DEFAULT_VARCHAR_SIZE default size to use when it cannot determine the field size from using sampleSize
#' @param file.sep the data file seperator to use - this function uses fread to detrmine the data field lengths, so i imagine you can pass any fields to fread
#' @param FAST_LOAD_AUTH_PATH the auth path for fasatload to use
#' @param PRIMARY_KEY_REGEX a list of regular expressions to use for primary key searching.  defaults to anyting that ends with _id or _key eg c(".*_id$",".*_key$")
#' @param checkpoint a number to checkpoint the fastload default 1e6
#' @param sessions number of session to use in fastload script
#' @param tenacity tenacity value to use in the fastload script
#' @param sleep sleep value to use in the fastload script
#' @param errlimit errlimit value to use in the fastload script
#' @param query_band the query band parameter to use set by DBA, typical UtilityDataSize=SMALL; to limit # of sessions by server
#' @param reservedWords the list of teradata reservered words
#' @param reservedWordAppendSuffix the suffix to append to a column named after a reserved word
#' @return tableName
#' @return mycols
#' @return keyCols
#' @return fastLoadScript
#' @return CREATE_TABLE_STATEMENT
#' @keywords fastload
#' @export
#' @examples
#' # generate the script
#' fastloadObject <-fastload(datafile = "C:\\TEMP\\20170810-TX1-provider.tsv")
#' # write the script to a file to view it
#'  t<-tempfile(fileext = ".fastload")
#'  cat(fastloadObject$fastLoadScript,file = t)
#'  shell(paste0("start", " ",t))
#'  # run the fastload file
#' shell(sprintf('fastload < "%s"', t))
#'
#'
fastload <-function(datafile, sampleSize=1e5, schema="ENTPR_EQA",PRIMARY_KEY_COLUMNS=c("auto"),tableName="",tableNamePrefix="TMP_",
                    override_column_sizes=list(".*name.*"="100",  ".*address.*"="100",  ".*description*"="100"),
                    DEFAULT_VARCHAR_SIZE=30, file.sep="\t", FAST_LOAD_AUTH_PATH="F:\\Data\\Settings\\FastLoad_Login.txt", PRIMARY_KEY_REGEX=c(".*_id$",".*_key$"), checkpoint=0,
                    sessions=10,tenacity=5,sleep=5,errlimit=50,query_band="UtilityDataSize=SMALL;",
                    reservedWords = c('abort','abortsession','abs','absolute','access_lock','account','acos','acosh','action','add','add_months','admin','after','aggregate','alias','all','allocate','alter','amp','and','ansidate','any','are','array','as','asc','asin','asinh','assertion',
                                      'at','atan','atan2','atanh','atomic','authorization','ave','average','avg','before','begin','between','binary','bit','blob','boolean','both','breadth','bt','but','by','byte','byteint','bytes','call','cascade','cascaded','case','case_n','casespecific',
                                      'cast','catalog','cd','char','char_length','char2hexint','character','character_length','characters','chars','check','checkpoint','class','clob','close','cluster','cm','coalesce','collate','collation','collect','column','comment','commit','completion',
                                      'compress','connect','connection','constraint','constraints','constructor','continue','convert_table_header','corr','corresponding','cos','cosh','count','covar_pop','covar_samp','create','cross','cs','csum','ct','cube','current','current_date',
                                      'current_path','current_role','current_time','current_timestamp','current_user','cursor','cv','cycle','data','database','datablocksize','date','dateform','day','deallocate','dec','decimal','declare','default','deferrable','deferred','degrees','del',
                                      'delete','depth','deref','desc','describe','descriptor','destroy','destructor','deterministic','diagnostic','diagnostics','dictionary','disabled','disconnect','distinct','do','domain','double','drop','dual','dump','dynamic','each','echo','else','elseif',
                                      'enabled','end','end-exec','eq','equals','error','errorfiles','errortables','escape','et','every','except','exception','exec','execute','exists','exit','exp','explain','external','extract','fallback','false','fastexport','fetch','file','first','float','for','foreign',
                                      'format','found','free','freespace','from','full','function','ge','general','generated','get','give','global','go','goto','grant','graphic','group','grouping','gt','handler','hash','hashamp','hashbakamp','hashbucket','hashrow','having','help','host','hour',
                                      'identity','if','ignore','immediate','in','inconsistent','index','indicator','initialize','initially','initiate','inner','inout','input','ins','insert','instead','int','integer','integerdate','intersect','interval','into','is','isolation','iterate','join',
                                      'journal','key','kurtosis','language','large','last','lateral','le','leading','leave','left','less','level','like','limit','ln','loading','local','localtime','localtimestamp','locator','lock','locking','log','logging','logon','long','loop','lower','lt','macro',
                                      'map','match','mavg','max','maximum','mcharacters','mdiff','merge','min','mindex','minimum','minus','minute','mlinreg','mload','mod','mode','modifies','modify','module','monitor','monresource','monsession','month','msubstr','msum','multiset','named','names','national',
                                      'natural','nchar','nclob','ne','new','new_table','next','no','none','not','nowait','null','nullif','nullifzero','numeric','object','objects','octet_length','of','off','old','old_table','on','only','open','operation','option','or','order','ordinality','out','outer','output',
                                      'over','overlaps','override','pad','parameter','parameters','partial','password','path','percent','percent_rank','perm','permanent','position','postfix','precision','prefix','preorder','prepare','preserve','primary','prior','private','privileges','procedure','profile',
                                      'proportional','protection','public','qualified','qualify','quantile','radians','random','range_n','rank','read','reads','real','recursive','ref','references','referencing','regr_avgx','regr_avgy','regr_count','regr_intercept','regr_r2','regr_slope','regr_sxx','regr_sxy',
                                      'regr_syy','relative','release','rename','repeat','replace','replication','repoverride','request','restart','restore','restrict','result','resume','ret','retrieve','return','returns','revalidate','revoke','right','rights','role','rollback','rollforward','rollup','routine',
                                      'row','row_number','rowid','rows','sample','sampleid','savepoint','schema','scope','scroll','search','second','section','sel','select','sequence','session','session_user','set','setresrate','sets','setsessrate','show','sin','sinh','size','skew','smallint','some','soundex',
                                      'space','specific','specifictype','spool','sql','sqlexception','sqlstate','sqltext','sqlwarning','sqrt','ss','start','startup','state','statement','static','statistics','stddev_pop','stddev_samp','stepinfo','string_cs','structure','subscriber','substr','substring','sum','summary',
                                      'suspend','system_user','table','tan','tanh','tbl_cs','temporary','terminate','than','then','threshold','time','timestamp','timezone_hour','timezone_minute','title','to','trace','trailing','transaction','translate','translate_chk','translation','treat','trigger','trim','true','type',
                                      'uc','undefined','under','undo','union','unique','unknown','unnest','until','upd','update','upper','uppercase','usage','user','using','value','values','var_pop','var_samp','varbyte','varchar','vargraphic','variable','varying','view','volatile','wait','when','whenever','where','while',
                                      'width_bucket','with','without','work','write','year','zeroifnull','zone'),
                    reservedWordAppendSuffix="_1"
                    ){

  # datafile="C:\\MASS_RESULTS\\chl17_detail_20180214_102654_eqa_mre_ce_a_e.txt";checkpoint=0; tenacity=5' sleep=5; errlimit=50; sampleSize=1e5; schema="ENTPR_EQA";PRIMARY_KEY_COLUMNS=c("auto");tableName="";tableNamePrefix="TMP_";override_column_sizes=list(".*name.*"="100",  ".*address.*"="100",  ".*description*"="100");  DEFAULT_VARCHAR_SIZE=30; file.sep="\t" ; FAST_LOAD_AUTH_PATH="F:\\Data\\Settings\\FastLoad_Login.txt";PRIMARY_KEY_REGEX=c(".*_id$",".*_key$");


  if (tableName=="")
  {
    # develop a table name..
    tableName<-removeExtension(basename(datafile))
    # remove any funny chars
    tableName<-gsub(pattern = "[^a-zA-Z0-9]",replacement = "_",x = tableName)
    tableName<-gsub(pattern = "[__]",replacement = "_",x = tableName)
    # add a prefix
    tableName<-paste0(tableNamePrefix,tableName)
  }
  ########################################################################
  #
  #  Create a template for the fastload tool.  This has significatnt performance
  #  improvements over JDBC or ODBC as it is multithreaded tool and loads ~10K records per second
  #
  FASTLOAD_TEMPLATE="SESSIONS [SESSIONS];
TENACITY [TENACITY];
SLEEP [SLEEP];
ERRLIMIT [ERRLIMIT];
/**
Dependancy: A .fastload file that contains the following information
.logon dbhostid/user,pass;

This is inside the globals$teradata$fastload$authpath, on windows usually F:\\Data\\Settings\\FastLoad_Login.txt

**/

.RUN [FAST_LOAD_AUTH_PATH]
SET QUERY_BAND='[QUERY_BAND]' UPDATE FOR SESSION ;
.SET RECORD VARTEXT '[CSV_FILE_SEPERATOR]' ;
RECORD 2; /* this skips first record in the source file */

DROP TABLE [TABLENAME];
DROP TABLE [TABLENAME]_error1;
DROP TABLE [TABLENAME]_error2;
[CREATE_TABLE_STATEMENT];

DEFINE
[CSV_DEFINES]


FILE=[CSV_FILENAME];
SHOW;
BEGIN LOADING [TABLENAME] ERRORFILES [TABLENAME]_error1,[TABLENAME]_error2
CHECKPOINT [CHECKPOINT];

INSERT INTO [TABLENAME]
VALUES
(
[INSERT_COLUMNS]
);

END LOADING;
.LOGOFF;
.QUIT;
";



  cmd<-sprintf('head -n %d "%s"',sampleSize,datafile)
  dfResult<-data.table::fread(colClasses = 'character',verbose=F,header = TRUE,strip.white=F, blank.lines.skip = FALSE, input = cmd,  sep = file.sep)
  names(dfResult)=make.names(names(dfResult),unique=TRUE)

  #
  # Note: We should rename the columns (or quote them)
  # macro new     begin    end    count
  # here we will add an underscore to the column name
  # # v'ALTER','INSERT','DECLARE','UPDATE','CREATE','DELETE','DROP' #cnt ,unknown,excl,variance
  # reservedWords <- c("macro", "new", "begin","end","count", "result", "create", "alter", "drop", "insert",
  #                    "declare", "update","delete","sel","from","cnt","select","unknown","excl","variance", "first","last")


  illeagalNamesIdx <- match(names(dfResult), reservedWords, nomatch=0)
  #flog.info(paste0("     Reserved words that are column names:",names(dfResult)[illeagalNamesIdx>=1]))
  names(dfResult)[illeagalNamesIdx>=1]<-paste0(names(dfResult)[illeagalNamesIdx>=1],reservedWordAppendSuffix)

  if ((nrow(dfResult)==0)==T)
  {
    stop(paste0(tableName, " contains 0 rows"))
  }


  #flog.info('     getting column sizes...')
  CSV_READ_COLUMNS=sapply(names(dfResult),function(x){
    paste0(x,' ','(VARCHAR(',255,'))')
  })



  SQL_INSERT_COLUMNS=sapply(names(dfResult),function(x){
    paste0(':',x)
  })

  # build the sql create statement

  mycols<-sapply(seq_along(names(dfResult)), function(index, names) {
    # index<-10
    x<-names[index]
    cls<-class(dfResult[[x]])
    if (cls %in% c("factor", "character", "integer")){

      foundOverride<-override_column_sizes[stringi::stri_detect(str = tolower(x = x), regex = names(override_column_sizes))]
      maxChars<-my.max(stringi::stri_length(dfResult[[x]]))
      if (length(foundOverride)>0)
      {
        maxChars<-foundOverride[[1]]
      }
      if (maxChars==0)
      {
        maxChars<-DEFAULT_VARCHAR_SIZE
      }
      tmp<-paste0(x,' VARCHAR (',maxChars,') ')
      return(tmp)

    }
    else
    {
      print(sprintf("Could not determine columns size %s %s",x,cls))
      tmp<-paste0(x,' VARCHAR (',DEFAULT_VARCHAR_SIZE,') ')
      return(tmp)
    }

  },names=names(dfResult))


  # uses the first column as the PRIMARY INDEX
  keyCols<-intersect(names(dfResult),PRIMARY_KEY_COLUMNS)
  # auto guess the primary key
  if (PRIMARY_KEY_COLUMNS=='auto')
  {
    keyCols<-names(dfResult)[stringi::stri_detect(str = tolower(x = names(dfResult)), regex = paste(PRIMARY_KEY_REGEX,collapse = "|"))]
  }
  # teradata requires something as a keycol, so if nothing is specified, use the first column..
  if ((length(keyCols)==0)==T){
    keyCols<-names(dfResult)[1]
  }




  CREATE_TABLE_STATEMENT<- paste('CREATE TABLE ', paste(schema,tableName,sep='.'), ' (',paste(mycols,collapse =',\n'),') PRIMARY INDEX ( ',paste(keyCols,collapse = ","),' )')


  fastLoadScript<-fromTemplateString(FASTLOAD_TEMPLATE,list(
    '[TABLENAME]'=paste(schema,tableName,sep='.'),
    '[CREATE_TABLE_STATEMENT]'= CREATE_TABLE_STATEMENT,
    '[FAST_LOAD_AUTH_PATH]'= FAST_LOAD_AUTH_PATH ,
    '[CSV_DEFINES]'= paste(CSV_READ_COLUMNS, collapse = '\n'),
    '[CSV_FILENAME]'= datafile,
    '[CSV_FILE_SEPERATOR]'= file.sep,
    '[INSERT_COLUMNS]'= paste(SQL_INSERT_COLUMNS, collapse = ',\n'),
    '[CHECKPOINT]'=sprintf("%d",checkpoint),
    '[SESSIONS]'=sprintf("%d",sessions),
    '[TENACITY]'=sprintf("%d",tenacity),
    '[SLEEP]'=sprintf("%d",sleep),
    '[ERRLIMIT]'=sprintf("%d",errlimit),
    '[QUERY_BAND]'=query_band
    )
  )

  return(list(tableName=tableName,mycols=mycols,keyCols=keyCols,fastLoadScript=fastLoadScript,CREATE_TABLE_STATEMENT=CREATE_TABLE_STATEMENT))


}


########################################################################
#
#' queue
#'
#' Simple Queue Interface
#'
#' @param statistics record statistics
#' @keywords queue
#' @export
#' @examples
#' # create a new queue
#' myQueue<-queue(TRUE)
#' for(i in 1:100){
#'   myQueue$enqueue(i)
#' }
#' myQueue$summary()
#' myQueue$resetStats()
#' for(i in 1:50){
#'   myQueue$dequeue()
#' }
#' myQueue$summary()
#' for(i in 1:26){
#'   myQueue$enqueue(letters[i])
#' }
#' myQueue$summary()


queue <- function(statistics=FALSE)
{
  CLAZZNAME<-'queue'
  thisEnv <- environment()
  q=list()
  assign("q",q,envir=thisEnv)
  assign("stats",NULL,envir=thisEnv)
  if(statistics){
    assign("stats",list(maxsize=0,on=0,off=0,minsize=0),envir=thisEnv)
  }

  me <- list(
    thisEnv = thisEnv,
    ## Define the accessors for the data fields.
    getEnv = function()
    {
      return(get("thisEnv",thisEnv))
    },
    getStats = function(){
      ### get the statistics from the queue
      return(get("stats",thisEnv))
    },

    resetStats=function(){
      ### reset the statistics from the queue
      q=get("q",envir=thisEnv)
      m=list(maxsize=length(q),minsize=length(q),on=0,off=0)
      assign("stats",m,envir=thisEnv)
      return(m)
    },
    enqueue=function(v){
      ###
      ### S3 Method for adding a value 'v' to the queue
      ###
      ## add the value to the list
      q=c(v,get("q",envir=thisEnv))
      ## stick the list back in the environment
      assign("q",q,envir=thisEnv)

      ## process the statistics if they're there:
      if(length({m=me$getStats()})>0){
        m$on=m$on+1
        if(length(q)>m$maxsize){
          m$maxsize=length(q)
        }
        assign("stats",m,envir=thisEnv)
      }

      return(v)
    },
    dequeue=function(){
      ###
      ### S3 Method for taking something off the list
      ###
      ## get the queue, check if anything on it:
      q=get("q",envir=thisEnv)
      if(length(q)==0){
        stop("Attempt to take element from empty queue")
      }

      ## take the last value
      v=q[[length(q)]]

      ## take the last value off:
      if(length(q)==1){
        assign("q",list(),thisEnv)
      }else{
        assign("q",q[1:(length(q)-1)],thisEnv)
      }
      ## update stats
      if(length({m=me$getStats()})>0){
        m$off=m$off+1
        if(length(get("q",envir=thisEnv))<m$minsize){
          m$minsize=length(q)-1
        }
        assign("stats",m,envir=thisEnv)
      }
      return(v)
    },
    ### print method
    print=function(x,...){
      print(get("q",envir=thisEnv))
    },
    length=function(){
      return(length(get("q",envir=thisEnv)))
    },
    summary=function(...){
      if(length({m=me$getStats()})>0){
        return(list(
          "length"=length(get("q",envir=thisEnv)),
          "added"=m$on,
          "removed"=m$off,
          "min" = m$minsize,
          "max" = m$maxsize
        ))
      }
      else
      {
        return(list(
          "length"=length(get("q",envir=thisEnv))
        ))
      }

    }


  )

  ## Define the value of the list within the current environment.
  assign('this',me,envir=thisEnv)

  ## Set the name for the class
  class(me) <- append(class(me),me$CLAZZNAME)
  return(me)
}

########################################################################
#
# required for flog to log both to console and file
# Get name of a parent function in call stack
# @param .where: where in the call stack. -1 means parent of the caller.
.get.parent.func.name <- function(.where) {
  the.function <- tryCatch(deparse(sys.call(.where - 1)[[1]]),
                           error=function(e) "(shell)")
  the.function <- ifelse(
    length(grep('flog\\.',the.function)) == 0, the.function, '(shell)')

  the.function
}
########################################################################
#
#' appender.file2
#'
#' logs to both a file and the console
#'
#' @param format the format to use
#' @param console log to console?
#' @param inherit inherits other logging proprerties
#' @param datetime the datetime
#' @param fmt the format to use
#' @keywords log
#' @export
#' @examples
#' flog.threshold(INFO)
#' flog.appender(appender.file2(paste0("logs/mylog-",Sys.Date(),".log"), console=TRUE))
#' flog.info("Started...")
########################################################################
#
# required for flog to log both to console and file
# Write to a dynamically-named file (and optionally the console), with inheritance
appender.file2 <- function(format, console=FALSE, inherit=TRUE,
                           datetime.fmt="%Y%m%dT%H%M%S") {
  .nswhere <- -3 # get name of the function 2 deep in the call stack
  # that is, the function that has called flog.*
  .funcwhere <- -3 # ditto for the function name
  .levelwhere <- -1 # ditto for the current "level"
  function(line) {
    if (console) cat(line, sep='')
    err <- function(e) {
      stop('Illegal function call, must call from flog.trace, flog.debug, flog.info, flog.warn, flog.error, flog.fatal, etc.')
    }
    the.level <- tryCatch(get("level", envir=sys.frame(.levelwhere)),error = err)
    the.threshold <- tryCatch(get('logger',envir=sys.frame(.levelwhere)), error=err)$threshold
    if(inherit) {
      LEVELS <- c(FATAL, ERROR, WARN, INFO, DEBUG, TRACE)
      levels <- names(LEVELS[the.level <= LEVELS & LEVELS <= the.threshold])
    } else levels <- names(the.level)
    the.time <- format(Sys.time(), datetime.fmt)
    the.namespace <- flog.namespace(.nswhere)
    the.namespace <- ifelse(the.namespace == 'futile.logger', 'ROOT', the.namespace)
    the.function <- .get.parent.func.name(.funcwhere)
    the.pid <- Sys.getpid()
    filename <- gsub('~t', the.time, format, fixed=TRUE)
    filename <- gsub('~n', the.namespace, filename, fixed=TRUE)
    filename <- gsub('~f', the.function, filename, fixed=TRUE)
    filename <- gsub('~p', the.pid, filename, fixed=TRUE)
    if(length(grep('~l', filename)) > 0) {
      sapply(levels, function(level) {
        filename <- gsub('~l', level, filename, fixed=TRUE)
        cat(line, file=filename, append=TRUE, sep='')
      })
    }else cat(line, file=filename, append=TRUE, sep='')
    invisible()
  }
}















########################################################################
#
#  hiveOdbc - RODBC hive object interface
#  Author: James_Banasiak@bcbsil.com
#
#

hiveOdbc <- function(host="twauslmnapp01.app.test.hcscint.net",user="", port=10001,
                     driver.name='auto',
                     driver.pattern='Hortonworks')
{
  CLAZZNAME<-'hiveOdbc'
  ## Get the environment for this instance of the function.
  thisEnv <- environment()
  ## Globals
  user<-user
  host<-host
  port<-port
  mode<-mode
  connected<-F
  driver<-''
  driver.name <- driver.name
  connection <- ''
  pwd<-''
  me <- list(
    thisEnv = thisEnv,
    ## Define the accessors for the data fields.
    getEnv = function()
    {
      return(get("thisEnv",thisEnv))
    },
    getDriver = function(){
      return(get("driver",thisEnv))
    },
    getMode = function(){
      return(get("mode",thisEnv))
    },
    getDriverName = function(){
      myDriver<-get("driver.name",thisEnv)
      pattern1<-get("driver.pattern",thisEnv)
      pattern2<-"ODBC"
      if (myDriver=='auto')
      {
        # auto specified, try to guess based on args supplied..
        odbcDriversInstalled<-names(readRegistry(key = "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers"))
        foundDrivers<-odbcDriversInstalled[grep(x = odbcDriversInstalled, pattern = paste0("^",pattern1,"$|",pattern1,"(.*)",pattern2,"(.*)"))]
        if (length(foundDrivers)==0)
        {
          stop("Cannot determine suitable driver in auto mode.  Driver list: ",paste(odbcDriversInstalled,collapse = ","))
        }
        print(paste0("[odbc driver] discovered ",foundDrivers[1]))
        return(foundDrivers[1])

      }
      else
      {
        return(myDriver)
      }

    },

    getPort = function(){
      return(get("port",thisEnv))
    },
    isConnected = function(){
      return(isTRUE(get("connected",thisEnv)))
    },
    getConnection = function(){
      return(get("connection",thisEnv))
    },
    getUser = function(){
      return(get("user",thisEnv))
    },
    getHost = function(){
      return(get("host",thisEnv))
    },
    password = function(){
      p<-getPass::getPass(msg=paste0(CLAZZNAME, " Enter [",me$getUser(),"] Password"))
      assign("pwd",base64enc::base64encode(what = charToRaw(p)),envir = thisEnv)
      return(p)
    },
    getSchemaInfo = function(schema='ENTPR_EQA'){


    },

    reconnect = function(){
      connected<-T
      #
      # Driver=Simba Hive ODBC Driver;Host=192.168.222.160; Port=10000;
      DSN <- sprintf("Driver=%s;Host=%s;Port=%s;AuthMech=3;UID=%s;PWD=%s",me$getDriverName(),me$getHost(),me$getPort(),me$getUser(),rawToChar(base64enc::base64decode(what = get("pwd",envir = thisEnv))))
      mycon <- tryCatch(mycon <-  odbcDriverConnect(DSN), error = function(e) {
        connected<<-F
        print(e)
      });
      assign("connected",connected,thisEnv)
      if (isTRUE(me$isConnected())){
        assign("connection",mycon,thisEnv)
      }else{
        stop("Not Connected!")
      }

    },
    connect = function(host ='', user = '', port=''){
      if (host!='')
      {
        # set host
        assign("host",host,me)
      }
      if (user!='')
      {
        # set username
        assign("user",user,me)
      }
      if (port!='')
      {
        # set username
        assign("port",port,me)
      }
      connected<-T
      DSN <- sprintf("Driver=%s;DBCName=%s;UID=%s;PWD=%s",me$getDriverName(),me$getHost(),me$getUser(),me$password())
      mycon <- tryCatch(mycon <-  odbcDriverConnect(DSN), error = function(e) {
        connected<<-F
        print(e)
      });
      assign("connected",connected,thisEnv)
      if (isTRUE(me$isConnected())){
        assign("connection",mycon,thisEnv)
      }else{
        stop("Not Connected!")
      }

    },
    disconnect = function(){
      if (isTRUE(me$isConnected())){
        RODBC::odbcClose(me$getConnection())
        assign("connected",F,thisEnv)
      }else{
        stop("Not Connected!")
      }

    },
    query = function(sql){
      if (isTRUE(me$isConnected())){
        q=removeSQLComments(sql)
        firstWord<-tolower(stringr::str_extract(string =q, pattern = '([A-Za-z]+)'))
        isUpdateQuery<-F

        # reservedWords <-
        if (firstWord %in% c("macro", "new", "begin","end","create", "alter", "drop", "insert", "declare", "update","delete","call","collect"))
        {
          isUpdateQuery<-T
        }


        res<-sqlQuery(me$getConnection(),q);


        if (isUpdateQuery==F && class(res)=='character')
        {
          stop(paste0('Error loading query: ',res))
        }
        return(res)

      }else{
        stop("Not Connected!")
      }

    }

  )

  ## Define the value of the list within the current environment.
  assign('this',me,envir=thisEnv)

  ## Set the name for the class
  class(me) <- append(class(me),me$CLAZZNAME)
  return(me)
}






.print.banner <-function(){
  cat(paste0("
                   _              _
                  | |            | |
   ___  __ _  __ _| |_ ___   ___ | |___
  / _ \\/ _` |/ _` | __/ _ \\ / _ \\| / __|
 |  __/ (_| | (_| | || (_) | (_) | \\__ \\
  \\___|\\__, |\\__,_|\\__\\___/ \\___/|_|___/
          | |
          |_|
                  loaded version v",packageVersion("eqatools"), "
             "))

}
# print our version
.print.banner()

# call document()
