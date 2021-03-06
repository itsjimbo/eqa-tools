% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/eqatools.R
\name{fromTemplateFile}
\alias{fromTemplateFile}
\title{fromTemplateFile}
\usage{
fromTemplateFile(file, varlist)
}
\arguments{
\item{file}{the template file to read}

\item{variableValueList}{a list of variables and values}
}
\description{
reads a file and replaces expressions with the format [variable] with a specified value
}
\examples{
sql<-fromTemplateFile('example.sql'),list(
  '[VAR1]'=c("VAL1"),
  '[VAR2]'=paste(1,2,3,sep=','),
  '[VAR3]'="VAL3"
))
print(sql)
debug.string(x = sql, fileext = '.sql')

}
\keyword{template}
