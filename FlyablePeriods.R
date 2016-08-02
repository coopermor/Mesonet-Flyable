library(parallel)
start.time <- Sys.time()
setwd("/MesonetData/")
files <- scan("Files.txt", what ="", sep ="\n") #MTS files to read

evaluateAg <- function(fLocation) 
{
  length <- nchar(fLocation)
  dateloc <- substr(fLocation, length-15, length-4) #Get station name and data date
  dframe <- read.csv(fLocation,skip=2,sep="")   #Read csv into data frame
  #Clean up data
  dframe[dframe==-999] <- NA   #Replace -999 with NA (Data flagged as bad by quality assurance routines.)
  dframe[dframe==-998] <- NA   #Replace -998 with NA (Sensor not installed.)
  dframe[dframe==-997] <- NA  #Replace -997 with NA (Cannot compute value due to missing calibration coefficient(s).)
  dframe[dframe==-996] <- NA  #Replace -996 with NA (Station did not report (missing).)
  dframe[dframe==-995] <- NA  #Replace -995 with NA (Data not reported on this time interval.)
  dframe[dframe==-994] <- NA  #Replace -994 with NA (	Value too wide to fit in column.)
  agCount=evalCount(dframe, 15, 18, 150,0,1435)  #Count data that meets parameters for Ag purposes
  colorCount=evalCount(dframe, 10, 15, 300,0,1435)  #Count data that meets parameters for Color image purposes
  consultCount = evalCount(dframe, 10, 10, 600,870,1230)  #Count data that meets parameters for Consultant purposes
  researchCount = evalCount(dframe, 5, 10, 600,930,1170)  #Count data that meets parameters for Research purposes
  result = data.frame(dateloc, agCount, colorCount, consultCount, researchCount)  #Write data to data frame
  return(result)
}

evalCount <- function(dF, WSPDt, WMAXt, SRADt,tMin,tMax)
{
  count = 0
  fly <- with(dF, WSPD<=WSPDt&WMAX<=WMAXt&SRADt<=SRAD&RAIN==0&TIME>=tMin&TIME<=tMax) #Create T/f vector of flyable periods
  fly[is.na(fly)] <- FALSE
  fly[289] <- FALSE
  count <- sum(diff(c(1,which(!fly)))%/%6) #Count consecutive periods of 30 minutes of TRUE
  return(count)
}
numCores = detectCores(logical=FALSE)
data <- mclapply(files, evaluateAg, mc.cores = numCores-1)
results <- do.call(rbind.data.frame, data)
write.csv(results, file="~/MesonetAnalysis/Results.csv")
end.time <- Sys.time()
time.taken <- end.time - start.time
print(time.taken)
