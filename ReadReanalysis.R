require(ncdf)
library(chron)
library(RPostgreSQL)
library(timeDate)


parseReanalysis <- function(file){
  ## pass a reanalysis file to this function and it parses it and writes out a CSV in the same directory with the suffix 'out2.csv'
  ncMax <- open.ncdf(file)
  ncMin <- open.ncdf(paste( "tmin", substr(file, 5, nchar( file ) ) , sep="") )
  tmaxArray <- get.var.ncdf( ncMax, "tmax")
  tminArray <- get.var.ncdf( ncMin, "tmin")
  dates <- chron(get.var.ncdf( ncMax, "time")/24, origin=c(month=1,day=1,year=1800))
 
  i <- 1
  outList <- list()
  system.time({
  for (y in 1:length(get.var.ncdf( ncMax, "lat"))){
    for (x in 1:length(get.var.ncdf( ncMax, "lon"))){
        outList[[i]] <- list()
        outList[[i]][1] <- get.var.ncdf( ncMax, "lon")[x]
        outList[[i]][2] <- get.var.ncdf( ncMax, "lat")[y]
        outList[[i]][[3]] <- tmaxArray[x, y,]
        outList[[i]][[4]] <- tminArray[x, y,]      
        outList[[i]][[5]] <- i #spatial key 
        i <- i + 1
    } 
    cat(y, "out of", length(get.var.ncdf( ncMax, "lat")), "\n")
  } 
  })

  ## take each lat, lon, and spatial key, 
  ## put them in a data frame and write it out

    df <- do.call(rbind, lapply( outList, function(x) data.frame( x[[5]], x[[2]], x[[1]])  ) )
    names(df) <- c("spatialKey", "lat", "lon")
    ## longitude is coded as 0-360... I presume they were on crack... so I fix it
    df$lon <- ifelse(df$lon > 180, df$lon-360, df$lon)
    write.csv(df, file = paste(file, ".spatial.key.csv", sep=""))

  
  system.time({
  for (i in 1:length(outList)){
    yearList <- outList[[i]]
    mat <- matrix(yearList[[3]])
    mat <- cbind(mat, yearList[[4]], 1:365, as.numeric(format(as.Date(dates[10]), "%Y") ), yearList[[5]])
    write.table(mat, file = paste(file, ".out2.csv", sep=""), append = TRUE, quote = FALSE, sep = ",",
            eol = "\n", na = "NA", dec = ".", row.names = FALSE,
            col.names = FALSE, qmethod = c("escape", "double"),
            fileEncoding = "")
    #dbWriteTable(con, "re2_temp", df, row.names=FALSE, append=TRUE)
  }
  
  })
}

require(multicore)


## below is how one might read in a full directory of tmax files. Follow the same pattern for other types

path <- "/my/path/"
setwd(path)

## parse all the files in the path and pull out the tmax files

filenames <- list.files(path, pattern ="^tmax.2m.gauss.[0-9].+[.]nc$")
## filenames <- filenames[1:2]


for ( file in filenames ){
  parseReanalysis( file )
}

## run this to do a bunch in parallel 
mclapply(filenames, parseReanalysis)

