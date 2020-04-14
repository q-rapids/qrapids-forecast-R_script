library(forecast)
library(elastic)
library(forecastHybrid)
library(prophet)

stringMethods <- c('ARIMA', 'ARIMA_FORCE_SEASONALITY', 'THETA', 'ETS', 'ETSDAMPED',
                   'BAGGEDETS', 'STL', 'NN', 'HYBRID', 'PROPHET')
directoryToSaveModels <- "forecastModels"
directoryToSaveForecasts <- "forecastsCache"
forecastsCacheLength <- 14

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

save <- function(name, index, method, element, directoryToSave) {
  cleanName <- gsub("[^[:alnum:] ]", "", name)
  dir.create(directoryToSave)
  filename <- paste(cleanName, index, method, sep = '_')
  filename <- paste(directoryToSave, filename, sep = '/')
  saveRDS(element, file = filename)
}

load <- function(name, index, method, directoryToLoadFrom) {
  cleanName <- gsub("[^[:alnum:] ]", "", name)
  filename <- paste(cleanName, index, method, sep = '_')
  filename <- paste(directoryToLoadFrom, filename, sep = '/')
  return(readRDS(filename))
}

checkModelExists <- function(name, index, method) {
  cleanName <- gsub("[^[:alnum:] ]", "", name)
  filename <- paste(cleanName, index, method, sep = '_')
  filename <- paste(directoryToSaveModels, filename, sep = '/')
  return(ifelse(file.exists(filename), TRUE, FALSE))
}

checkForecastCache <- function(name, index, method, horizon) {
  print("CHECKING CACHE...")
  cleanName <- gsub("[^[:alnum:] ]", "", name)
  filename <- paste(cleanName, index, method, sep = '_')
  filename <- paste(directoryToSaveForecasts, filename, sep = '/')
  if (file.exists(filename)) {
    forecasts <- readRDS(filename)
    cacheValid <- ifelse(length(forecasts$mean)>=horizon, TRUE, FALSE)
    if (cacheValid == TRUE) {
      print("CACHE VALID!")
      return(TRUE)
    } else {
      return(FALSE)
    }
  } else {
    print("NOT USING CACHE")
      return(FALSE)
  }
}

trainArimaModel <- function(name, index, forceSeasonality, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  arimaModel <- NULL
  method=ifelse(forceSeasonality == TRUE, stringMethods[2], stringMethods[1])
  arimaD=ifelse(forceSeasonality == TRUE, 1, NA)
  
  arimaModel <- auto.arima(timeseries, D=arimaD, stepwise = FALSE, approximation = FALSE)
  save(name, index, method, arimaModel, directoryToSaveModels)
  flist <- forecastArima(arimaModel, forecastsCacheLength) # STORE FORECAST CACHE
  save(name, index, method, flist, directoryToSaveForecasts)
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
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, subset, end = horizon)) # SUBSET FIRST 0,HORIZON ELEMENTS
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastArima(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainArimaModel(name, index, forceSeasonality, frequencyts)
    flist <- forecastArima(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  }
  return(flist)
}

trainThetaModel <- function(name, index, frequencyts) {
  method <- stringMethods[3]
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  thetaModel <- thetam(timeseries)
  save(name, index, method, thetaModel, directoryToSaveModels)
  flist <- forecastTheta(thetaModel, forecastsCacheLength) # STORE FORECAST CACHE
  save(name, index, method, flist, directoryToSaveForecasts)
  return(thetaModel)
}

forecastTheta <- function(model, horizon) {
  f <- forecast(model, h = horizon)
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastThetaWrapper <- function(name, index, frequencyts, horizon) {
  method <- stringMethods[3]
  model <- NULL
  if(checkModelExists(name, index, method)) {
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, FUN=function(x) x[1:horizon]))    #(flist, subset, end = horizon)) # SUBSET FIRST 0,HORIZON ELEMENTS
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastTheta(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainThetaModel(name, index, frequencyts)
    flist <- forecastTheta(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  }
  return(flist)
}

trainETSModel <- function(name, index, forceDamped, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  etsModel <- ets(timeseries, damped = forceDamped)
  method <- ifelse(forceDamped, stringMethods[5], stringMethods[4])
  save(name, index, method, etsModel, directoryToSaveModels)
  flist <- forecastETS(etsModel, forecastsCacheLength) # STORE FORECAST CACHE
  save(name, index, method, flist, directoryToSaveForecasts)
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
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, subset, end = horizon)) # SUBSET FIRST 0,HORIZON ELEMENTS
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastETS(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainETSModel(name, index, forceDamped, frequencyts)
    flist <- forecastETS(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  }
  return(flist)
}

trainBaggedETSModel <- function(name, index, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  baggedETSModel <- baggedETS(timeseries)
  method <- stringMethods[6]
  save(name, index, method, baggedETSModel, directoryToSaveModels)
  flist <- forecastBaggedETS(baggedETSModel, forecastsCacheLength)
  save(name, index, method, flist, directoryToSaveForecasts)
  return(baggedETSModel)
}

forecastBaggedETS <- function(model, horizon) {
  f <- forecast(model, h = horizon)
  flist <- list("lower1" = f$lower, "lower2" = f$lower, "mean" = f$mean, 
                "upper1" = f$upper, "upper2" = f$upper)
  return(flist)
}

forecastBaggedETSWrapper <- function(name, index, frequencyts, horizon) {
  method <- stringMethods[6]
  model <- NULL
  if(checkModelExists(name, index, method)) {
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, subset, end = horizon)) # lapply(flist, FUN=function(x) x[1:horizon])
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastBaggedETS(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainBaggedETSModel(name, index, frequencyts)
    flist <- forecastBaggedETS(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  }
  return(flist)
}

trainSTLModel <- function(name, index, frequencyts) {
  method <- stringMethods[7]
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  STLModel <- mstl(timeseries)
  save(name, index, method, STLModel, directoryToSaveModels)
  flist <- forecastSTL(STLModel, forecastsCacheLength) # STORE FORECAST CACHE
  save(name, index, method, flist, directoryToSaveForecasts)
  return(STLModel)
}

forecastSTL <- function(model, horizon) {
  f <- forecast(model, h = horizon)
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastSTLWrapper <- function(name, index, frequencyts, horizon) {
  method <- stringMethods[7]
  model <- NULL
  if(checkModelExists(name, index, method)) {
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, subset, end = horizon)) # lapply(flist, FUN=function(x) x[1:horizon])
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastArima(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainSTLModel(name, index, frequencyts)
    flist <- forecastSTL(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  }
  return(flist)
}

trainNNModel <- function(name, index, frequencyts) {
  method <- stringMethods[8]
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  NNModel <- nnetar(timeseries)
  save(name, index, method, NNModel, directoryToSaveModels)
  flist <- forecastNN(NNModel, forecastsCacheLength) # STORE FORECAST CACHE
  save(name, index, method, flist, directoryToSaveForecasts)
  return(NNModel)
}

forecastNN <- function(model, horizon) {
  f <- forecast(model, h = horizon, PI = TRUE)
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastNNWrapper <- function(name, index, frequencyts, horizon) {
  method <- stringMethods[8]
  model <- NULL
  if(checkModelExists(name, index, method)) {
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, subset, end = horizon)) # lapply(flist, FUN=function(x) x[1:horizon])
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastNN(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainNNModel(name, index, frequencyts)
    flist <- forecastNN(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  }
  return(flist)
}

trainHybridModel <- function(name, index, cvHorizon, frequencyts) {
  timeseries <- searchElement(name, index, frequencyts, returnDF = FALSE)
  method <- stringMethods[9]
  hybridCVModel <- hybridModel(timeseries,
                   lambda = "auto", 
                   windowSize = (length(timeseries)-cvHorizon*2),
                   weights = "cv.errors", cvHorizon = cvHorizon,
                   horizonAverage = TRUE, 
                   a.args = list(stepwise = FALSE, trace = FALSE),
                   e.args = list(allow.multiplicative.trend = TRUE),
                   parallel = TRUE,
                   num.cores = 2)
  save(name, index, method, hybridCVModel, directoryToSaveModels)
  flist <- forecastArima(hybridCVModel, forecastsCacheLength) # STORE FORECAST CACHE
  save(name, index, method, flist, directoryToSaveForecasts)
  return(hybridCVModel)
}

forecastHybrid <- function(model, horizon) {
  f <- forecast(model, h = horizon, PI.combination = "mean")
  flist <- list("lower1" = f$lower[,1], "lower2" = f$lower[,2], "mean" = f$mean,
                "upper1" = f$upper[,1], "upper2" = f$upper[,2])
  return(flist)
}

forecastHybridWrapper <- function(name, index, frequencyts, horizon) {
  method <- stringMethods[9]
  model <- NULL
  if(checkModelExists(name, index, method)) {
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, FUN=function(x) x[1:horizon]))
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastHybrid(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainHybridModel(name, index, horizon, frequencyts)
    flist <- forecastHybrid(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  } #plot(model, type="fit")
  return(flist)
}

trainProphetModel <- function(name, index) {
  df <- searchElement(name, index, 7, returnDF = TRUE)
  method <- stringMethods[10]
  prophetModel <- prophet(df, daily.seasonality = 'auto', weekly.seasonality = 'auto')
  save(name, index, method, prophetModel, directoryToSaveModels)
  flist <- forecastProphet(prophetModel, forecastsCacheLength) # STORE FORECAST CACHE
  save(name, index, method, flist, directoryToSaveForecasts)
  return(prophetModel)
}

forecastProphet <- function(model, horizon) {
  future <- make_future_dataframe(model, periods = horizon, freq = 'day', include_history = FALSE)
  f <- predict(model, future)
  flist <- list("lower1" = f$yhat_lower, "lower2" = f$yhat_lower, "mean" = f$yhat,
                "upper1" = f$yhat_upper, "upper2" = f$yhat_upper)  
  return(flist)
}

forecastProphetWrapper <- function(name, index, horizon) {
  method <- stringMethods[10]
  model <- NULL
  if(checkModelExists(name, index, method)) {
    if(checkForecastCache(name, index, method, horizon)) { # IF MODEL EXISTS, CHECK THE FORECAST CACHE
      flist <- load(name, index, method, directoryToSaveForecasts) 
      return(lapply(flist, FUN=function(x) x[1:horizon])) # lapply(flist, FUN=function(x) x[1:horizon])
    } else {
        model <- load(name, index, method, directoryToSaveModels)
        flist <- forecastProphet(model, horizon)
        save(name, index, method, flist, directoryToSaveForecasts)
    }
  } else {
    model <- trainProphetModel(name, index)
    flist <- forecastProphet(model, horizon)
    if(horizon > forecastsCacheLength) {
      save(name, index, method, flist, directoryToSaveForecasts)
    }
  }
  return(flist)
}