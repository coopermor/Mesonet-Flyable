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
  agCount=evalReason(dframe, 15, 18, 150,0,1435)  #Count data that meets parameters for Ag purposes
  colorCount=evalReason(dframe, 10, 15, 300,0,1435)  #Count data that meets parameters for Color image purposes
  consultCount = evalReasonTime(dframe, 10, 10, 600,870,1230)  #Count data that meets parameters for Consultant purposes
  researchCount = evalReasonTime(dframe, 5, 10, 600,930,1170)  #Count data that meets parameters for Research purposes
  result = data.frame(dateloc, agCount, colorCount, consultCount, researchCount)  #Write data to data frame
  return(result)
}

evalReason <-function(dF, WSPDt, WMAXt, SRADt, tMin, tMax)
{
  flyWind <- with(dF, WSPD<=WSPDt) #Create T/F vectors of observations meeting conditions
  flyGust <- with(dF, WMAX<=WMAXt)
  flySolar <- with(dF, SRADt<=SRAD)
  flyRain <- with(dF, RAIN==0)
  wind = sum(!flyWind) #Count number of condtions
  gust = sum(!flyGust)
  solar = sum(!flySolar)
  rain = sum(!flyRain)
  total = length(flyWind)
  result = data.frame(wind, gust, solar, rain, total) #Return data frame with number of periods not meeting each condtion
  return(result)
}

#EvalReasonTime works the same as evalReason except restricting based on times. Could have been written as one function but chose not to
evalReasonTime <-function(dF, WSPDt, WMAXt, SRADt, tMin, tMax)
{
  rowsToKeep<- with(dF, TIME>=tMin&TIME<=tMax)
  dF = dF[rowsToKeep,]
  flyWind <- with(dF, WSPD<=WSPDt)
  flyGust <- with(dF, WMAX<=WMAXt)
  flySolar <- with(dF, SRADt<=SRAD)
  flyRain <- with(dF, RAIN==0)
  wind = sum(!flyWind)
  gust = sum(!flyGust)
  solar = sum(!flySolar)
  rain = sum(!flyRain)
  total = length(flyWind)
  result = data.frame(wind, gust, solar, rain, total)
  return(result)
}
numcores = detectCores(LOGICAL=FALSE)
data <- mclapply(files, evaluateAg, mc.cores=numcores-1)
results <- do.call(rbind.data.frame, data)
write.csv(results, file="~/MesonetAnalysis/TestResults/Results.csv")
end.time <- Sys.time()
time.taken <- end.time - start.time
print(time.taken)
