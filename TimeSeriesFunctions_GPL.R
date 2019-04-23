if(!require(forecast)){
  install.packages("forecast", repos='https://cloud.r-project.org')
  library(forecast)
}
if(!require(devtools)){
  install.packages("devtools", repos='https://cloud.r-project.org')
  library(devtools)
}
if(!require(elastic)){
  install_version("elastic", version="0.8.4", repos='https://cloud.r-project.org')
  library(elastic)
}
if(!require(forecastHybrid)){
  install.packages("forecastHybrid", repos='https://cloud.r-project.org')
  library(forecastHybrid)
}
if(!require(prophet)){
  install.packages("prophet", repos='https://cloud.r-project.org')
  library(prophet)
}

stringMethods <- c('ARIMA', 'ARIMA_FORCE_SEASONALITY', 'THETA', 'ETS', 'ETSDAMPED',
                   'BAGGEDETS', 'STL', 'NN', 'HYBRID', 'PROPHET')
directoryToSave <- "forecastModels"

getAvailableMethods <- function() {
  stringMethods
}

elasticConnection <- function(host, path, user, pwd, port) {
  # CONNECTION TO ELASTICSEARCH NODE
  connect(es_host = host, es_path = path, es_user= user, es_pwd = pwd,
          es_port = port, es_transport_schema  = "http")
  ping()
}

searchElement <- function(name, index, tsfrequency, returnDF) {
  # SEARCH FOR A NORMALIZED ELEMENT AND RETURN THE ASSOCIATED TIME SERIES
  searchString <- ifelse(grepl("metrics", index, fixed=TRUE), 'metric:', 
                         ifelse(grepl("factors", index, fixed=TRUE), 'factor:', 'strategic_indicator:'))
  esearch <- Search(index = index, q = paste(searchString, name, sep = ''),
                    sort = "evaluationDate:asc", source = "value,evaluationDate", size = 10000)$hits$hits
  valuesEsearch <- sapply(esearch, function(x) as.numeric(x$`_source`$value))
  
  if (returnDF == FALSE) {
    timeseries <- ts(valuesEsearch, frequency = tsfrequency, start = 0)
    return(timeseries)
  } else {
  datesEsearch <- sapply(esearch, function(x) as.character(x$`_source`$evaluationDate))
  datesEsearch <- as.Date(datesEsearch)
  df <- data.frame("ds" = datesEsearch, "y" = valuesEsearch)
  return(df)
  }
}

saveModel <- function(name, index, method, model) {
  cleanName <- gsub("[^[:alnum:] ]", "", name)
  dir.create(directoryToSave)
  filename <- paste(cleanName, index, method, sep = '_')
  filename <- paste(directoryToSave, filename, sep = '/')
  saveRDS(model, file = filename)
}

loadModel <- function(name, index, method) {
  cleanName <- gsub("[^[:alnum:] ]", "", name)
  filename <- paste(cleanName, index, method, sep = '_')
  filename <- paste(directoryToSave, filename, sep = '/')
  return(readRDS(filename))
}

checkModelExists <- function(name, index, method) {
  cleanName <- gsub("[^[:alnum:] ]", "", name)
  filename <- paste(cleanName, index, method, sep = '_')
  filename <- paste(directoryToSave, filename, sep = '/')
  return(ifelse(file.exists(filename), TRUE, FALSE))
}

trainArimaModel <- function(name, index, forceSeasonality, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  arimaModel <- NULL
  if (forceSeasonality == FALSE) {
    arimaModel <- auto.arima(timeseries, stepwise = FALSE, approximation = FALSE)
    saveModel(name, index, stringMethods[1], arimaModel)
  }
  if (forceSeasonality == TRUE) {
    arimaModel <- auto.arima(timeseries, D=1, stepwise = FALSE, approximation = FALSE)
    saveModel(name, index, stringMethods[2], arimaModel)
  }
  return(arimaModel)
}

forecastArima <- function(model, horizon) {
  f <- forecast(model, h = horizon)
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastArimaWrapper <- function(name, index, forceSeasonality, frequencyts, horizon) {
  method <- ifelse(forceSeasonality, stringMethods[2], stringMethods[1])
  model <- NULL
  if(checkModelExists(name, index, method)) {
    model <- loadModel(name, index, method)
  } else {
    model <- trainArimaModel(name, index, forceSeasonality, frequencyts)
  }
  return(forecastArima(model, horizon))
}

trainThetaModel <- function(name, index, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  thetaModel <- thetam(timeseries)
  saveModel(name, index, stringMethods[3], thetaModel)
  return(thetaModel)
}

forecastTheta <- function(model, horizon) {
  f <- forecast(model, h = horizon)
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastThetaWrapper <- function(name, index, frequencyts, horizon) {
  model <- NULL
  if(checkModelExists(name, index, stringMethods[3])) {
    model <- loadModel(name, index, stringMethods[3])
  } else {
    model <- trainThetaModel(name, index, frequencyts)
  }
  return(forecastTheta(model, horizon))
}

trainETSModel <- function(name, index, forceDamped, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  etsModel <- ets(timeseries, damped = forceDamped)
  method <- ifelse(forceDamped, stringMethods[5], stringMethods[4])
  saveModel(name, index, method, etsModel)
  return(etsModel)
}

forecastETS <- function(model, horizon) {
  f <- forecast(model, h = horizon, method = 'ets')
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastETSWrapper <- function(name, index, forceDamped, frequencyts, horizon) {
  method <- ifelse(forceDamped, stringMethods[5], stringMethods[4])
  model <- NULL
  if(checkModelExists(name, index, method)) {
    model <- loadModel(name, index, method)
  } else {
    model <- trainETSModel(name, index, forceDamped, frequencyts)
  }
  return(forecastETS(model, horizon))
}

trainBaggedETSModel <- function(name, index, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  baggedETSModel <- baggedETS(timeseries)
  saveModel(name, index, stringMethods[6], baggedETSModel)
  return(baggedETSModel)
}

forecastBaggedETS <- function(model, horizon) {
  f <- forecast(model, h = horizon)
  flist <- list("lower1" = f$lower, "lower2" = f$lower, "mean" = f$mean, 
                "upper1" = f$upper, "upper2" = f$upper)
  return(flist)
}

forecastBaggedETSWrapper <- function(name, index, frequencyts, horizon) {
  model <- NULL
  if(checkModelExists(name, index, stringMethods[6])) {
    model <- loadModel(name, index, stringMethods[6])
  } else {
    model <- trainBaggedETSModel(name, index, frequencyts)
  }
  return(forecastBaggedETS(model, horizon))
}

trainSTLModel <- function(name, index, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  STLModel <- mstl(timeseries)
  saveModel(name, index, stringMethods[7], STLModel)
  return(STLModel)
}

forecastSTL <- function(model, horizon) {
  f <- forecast(model, h = horizon)
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastSTLWrapper <- function(name, index, frequencyts, horizon) {
  model <- NULL
  if(checkModelExists(name, index, stringMethods[7])) {
    model <- loadModel(name, index, stringMethods[7])
  } else {
    model <- trainSTLModel(name, index, frequencyts)
  }
  return(forecastSTL(model, horizon))
}

trainNNModel <- function(name, index, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  NNModel <- nnetar(timeseries)
  saveModel(name, index, stringMethods[8], NNModel)
  return(NNModel)
}

forecastNN <- function(model, horizon) {
  f <- forecast(model, h = horizon, PI = TRUE)
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastNNWrapper <- function(name, index, frequencyts, horizon) {
  model <- NULL
  if(checkModelExists(name, index, stringMethods[8])) {
    model <- loadModel(name, index, stringMethods[8])
  } else {
    model <- trainNNModel(name, index, frequencyts)
  }
  return(forecastNN(model, horizon))
}

trainHybridModel <- function(name, index, cvHorizon, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  hybridCVModel <- hybridModel(timeseries,
                   lambda = "auto", 
                   windowSize = (length(timeseries)-cvHorizon*2),
                   weights = "cv.errors", cvHorizon = cvHorizon,
                   horizonAverage = TRUE, 
                   a.args = list(stepwise = FALSE, trace = FALSE),
                   e.args = list(allow.multiplicative.trend = TRUE),
                   parallel = TRUE,
                   num.cores = 2)
  saveModel(name, index, stringMethods[9], hybridCVModel)
  return(hybridCVModel)
}

forecastHybrid <- function(model, horizon) {
  f <- forecast(model, h = horizon, PI.combination = "mean")
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastHybridWrapper <- function(name, index, frequencyts, horizon) {
  model <- NULL
  if(checkModelExists(name, index, stringMethods[9])) {
    model <- loadModel(name, index, stringMethods[9])
  } else {
    model <- trainHybridModel(name, index, horizon, frequencyts)
  } #plot(model, type="fit")
  return(forecastHybrid(model, horizon))
}

trainProphetModel <- function(name, index) {
  df <- searchElement(name, index, 7, returnDF = TRUE)
  model <- prophet(df, daily.seasonality = 'auto', weekly.seasonality = 'auto')
  saveModel(name, index, stringMethods[10], model)
  return(model)
}

forecastProphet <- function(model, horizon) {
  future <- make_future_dataframe(model, periods = horizon, freq = 'day', include_history = FALSE)
  f <- predict(model, future)
  flist <- list("lower1" = f$yhat_lower, "lower2" = f$yhat_lower, "mean" = f$yhat,
                "upper1" = f$yhat_upper, "upper2" = f$yhat_upper)  
  return(flist)
}

forecastProphetWrapper <- function(name, index, horizon) {
  model <- NULL
  if(checkModelExists(name, index, stringMethods[10])) {
    model <- loadModel(name, index, stringMethods[10])
  } else {
    model <- trainProphetModel(name, index)
  }
  return(forecastProphet(model, horizon))
}