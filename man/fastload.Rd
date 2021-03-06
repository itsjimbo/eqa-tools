% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/eqatools.R
\name{fastload}
\alias{fastload}
\title{fastload a file or dataset into teradata}
\usage{
fastload(datafile, sampleSize = 1e+05, schema = "ENTPR_EQA",
  PRIMARY_KEY_COLUMNS = c("auto"), tableName = "",
  tableNamePrefix = "TMP_", override_column_sizes = list(`.*name.*` = "100",
  `.*address.*` = "100", `.*description*` = "100"), DEFAULT_VARCHAR_SIZE = 30,
  file.sep = "\\t",
  FAST_LOAD_AUTH_PATH = "F:\\\\Data\\\\Settings\\\\FastLoad_Login.txt",
  PRIMARY_KEY_REGEX = c(".*_id$", ".*_key$"), checkpoint = 0,
  sessions = 10, tenacity = 5, sleep = 5, errlimit = 50,
  query_band = "UtilityDataSize=SMALL;", reservedWords = c("abort",
  "abortsession", "abs", "absolute", "access_lock", "account", "acos", "acosh",
  "action", "add", "add_months", "admin", "after", "aggregate", "alias", "all",
  "allocate", "alter", "amp", "and", "ansidate", "any", "are", "array", "as",
  "asc", "asin", "asinh", "assertion", "at", "atan", "atan2", "atanh", "atomic",
  "authorization", "ave", "average", "avg", "before", "begin", "between",
  "binary", "bit", "blob", "boolean", "both", "breadth", "bt", "but", "by",
  "byte", "byteint", "bytes", "call",      "cascade", "cascaded", "case",
  "case_n", "casespecific", "cast", "catalog", "cd", "char", "char_length",
  "char2hexint", "character", "character_length", "characters", "chars",
  "check", "checkpoint", "class", "clob", "close", "cluster", "cm", "coalesce",
  "collate", "collation", "collect", "column", "comment", "commit",
  "completion", "compress", "connect", "connection", "constraint",
  "constraints", "constructor", "continue", "convert_table_header", "corr",
  "corresponding", "cos", "cosh", "count",      "covar_pop", "covar_samp",
  "create", "cross", "cs", "csum", "ct", "cube", "current", "current_date",
  "current_path", "current_role", "current_time", "current_timestamp",
  "current_user", "cursor", "cv", "cycle", "data", "database", "datablocksize",
  "date", "dateform", "day", "deallocate", "dec", "decimal", "declare",
  "default", "deferrable", "deferred", "degrees", "del", "delete", "depth",
  "deref", "desc", "describe", "descriptor", "destroy", "destructor",
  "deterministic", "diagnostic", "diagnostics",      "dictionary", "disabled",
  "disconnect", "distinct", "do", "domain", "double", "drop", "dual", "dump",
  "dynamic", "each", "echo", "else", "elseif", "enabled", "end", "end-exec",
  "eq", "equals", "error", "errorfiles", "errortables", "escape", "et", "every",
  "except", "exception", "exec", "execute", "exists", "exit", "exp", "explain",
  "external", "extract", "fallback", "false", "fastexport", "fetch", "file",
  "first", "float", "for", "foreign", "format", "found", "free", "freespace",
  "from", "full",      "function", "ge", "general", "generated", "get", "give",
  "global", "go", "goto", "grant", "graphic", "group", "grouping", "gt",
  "handler", "hash", "hashamp", "hashbakamp", "hashbucket", "hashrow", "having",
  "help", "host", "hour", "identity", "if", "ignore", "immediate", "in",
  "inconsistent", "index", "indicator", "initialize", "initially", "initiate",
  "inner", "inout", "input", "ins", "insert", "instead", "int", "integer",
  "integerdate", "intersect", "interval", "into", "is", "isolation", "iterate", 
      "join", "journal", "key", "kurtosis", "language", "large", "last",
  "lateral", "le", "leading", "leave", "left", "less", "level", "like", "limit",
  "ln", "loading", "local", "localtime", "localtimestamp", "locator", "lock",
  "locking", "log", "logging", "logon", "long", "loop", "lower", "lt", "macro",
  "map", "match", "mavg", "max", "maximum", "mcharacters", "mdiff", "merge",
  "min", "mindex", "minimum", "minus", "minute", "mlinreg", "mload", "mod",
  "mode", "modifies", "modify", "module", "monitor",      "monresource",
  "monsession", "month", "msubstr", "msum", "multiset", "named", "names",
  "national", "natural", "nchar", "nclob", "ne", "new", "new_table", "next",
  "no", "none", "not", "nowait", "null", "nullif", "nullifzero", "numeric",
  "object", "objects", "octet_length", "of", "off", "old", "old_table", "on",
  "only", "open", "operation", "option", "or", "order", "ordinality", "out",
  "outer", "output", "over", "overlaps", "override", "pad", "parameter",
  "parameters", "partial", "password", "path",      "percent", "percent_rank",
  "perm", "permanent", "position", "postfix", "precision", "prefix", "preorder",
  "prepare", "preserve", "primary", "prior", "private", "privileges",
  "procedure", "profile", "proportional", "protection", "public", "qualified",
  "qualify", "quantile", "radians", "random", "range_n", "rank", "read",
  "reads", "real", "recursive", "ref", "references", "referencing", "regr_avgx",
  "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope",
  "regr_sxx", "regr_sxy", "regr_syy",      "relative", "release", "rename",
  "repeat", "replace", "replication", "repoverride", "request", "restart",
  "restore", "restrict", "result", "resume", "ret", "retrieve", "return",
  "returns", "revalidate", "revoke", "right", "rights", "role", "rollback",
  "rollforward", "rollup", "routine", "row", "row_number", "rowid", "rows",
  "sample", "sampleid", "savepoint", "schema", "scope", "scroll", "search",
  "second", "section", "sel", "select", "sequence", "session", "session_user",
  "set", "setresrate",      "sets", "setsessrate", "show", "sin", "sinh",
  "size", "skew", "smallint", "some", "soundex", "space", "specific",
  "specifictype", "spool", "sql", "sqlexception", "sqlstate", "sqltext",
  "sqlwarning", "sqrt", "ss", "start", "startup", "state", "statement",
  "static", "statistics", "stddev_pop", "stddev_samp", "stepinfo", "string_cs",
  "structure", "subscriber", "substr", "substring", "sum", "summary", "suspend",
  "system_user", "table", "tan", "tanh", "tbl_cs", "temporary", "terminate",
  "than", "then",      "threshold", "time", "timestamp", "timezone_hour",
  "timezone_minute", "title", "to", "trace", "trailing", "transaction",
  "translate", "translate_chk", "translation", "treat", "trigger", "trim",
  "true", "type", "uc", "undefined", "under", "undo", "union", "unique",
  "unknown", "unnest", "until", "upd", "update", "upper", "uppercase", "usage",
  "user", "using", "value", "values", "var_pop", "var_samp", "varbyte",
  "varchar", "vargraphic", "variable", "varying", "view", "volatile", "wait",
  "when",      "whenever", "where", "while", "width_bucket", "with", "without",
  "work", "write", "year", "zeroifnull", "zone"),
  reservedWordAppendSuffix = "_1")
}
\arguments{
\item{datafile}{the file to load, must have eol usually tab or comma seperated}

\item{sampleSize}{the sample to take from the file to determine column sizes}

\item{schema}{the schema to use when generating the fastload script}

\item{PRIMARY_KEY_COLUMNS}{list of columns specified as the primary key.  Default is "auto" which will auto-guess any column ending with PRIMARY_KEY_REGEX.  It will use the first column if nothing is specified which may slow indexing down or create wasted space}

\item{tableName}{the final tablename to use.  If omitted it will develop a table name based off the base name of the filename and prefix, eg a filename of myfilename123 will be TMP_myfilename123}

\item{tableNamePrefix}{table naming prefix to create if}

\item{override_column_sizes}{list of columns and grep expressions to use when determining columns sizes.  eg .*name.*="100" will force any field with the word name to be 100 bytes}

\item{DEFAULT_VARCHAR_SIZE}{default size to use when it cannot determine the field size from using sampleSize}

\item{file.sep}{the data file seperator to use - this function uses fread to detrmine the data field lengths, so i imagine you can pass any fields to fread}

\item{FAST_LOAD_AUTH_PATH}{the auth path for fasatload to use}

\item{PRIMARY_KEY_REGEX}{a list of regular expressions to use for primary key searching.  defaults to anyting that ends with _id or _key eg c(".*_id$",".*_key$")}

\item{checkpoint}{a number to checkpoint the fastload default 1e6}

\item{sessions}{number of session to use in fastload script}

\item{tenacity}{tenacity value to use in the fastload script}

\item{sleep}{sleep value to use in the fastload script}

\item{errlimit}{errlimit value to use in the fastload script}

\item{query_band}{the query band parameter to use set by DBA, typical UtilityDataSize=SMALL; to limit # of sessions by server}

\item{reservedWords}{the list of teradata reservered words}

\item{reservedWordAppendSuffix}{the suffix to append to a column named after a reserved word}
}
\value{
tableName

mycols

keyCols

fastLoadScript

CREATE_TABLE_STATEMENT
}
\description{
fastload a file or dataset into teradata
}
\examples{
# generate the script
fastloadObject <-fastload(datafile = "C:\\\\TEMP\\\\20170810-TX1-provider.tsv")
# write the script to a file to view it
 t<-tempfile(fileext = ".fastload")
 cat(fastloadObject$fastLoadScript,file = t)
 shell(paste0("start", " ",t))
 # run the fastload file
shell(sprintf('fastload < "\%s"', t))


}
\keyword{fastload}
