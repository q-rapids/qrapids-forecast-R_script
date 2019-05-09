# R scripts for forecasting in Q-Rapids

R script file including a set of forecasting methods. This script acts as a wrapper of some state of the art forecasting methods, implemented in packages like the well-known **forecast**. Apart from that, it also implements some other useful aspects, like wrapping the **elastic** package to allow easy metrics, factors, and strategic indicators gathering from an elasticsearch DB. The script also implements saving and reusing the fitted forecasting models, in order to save time and computation resources.

The script is intended  to be used along with the [q-rapids-forecast](https://github.com/q-rapids/qrapids-forecast) library, but is prepared to be used alone as well.


# Available forecasting methods

The available methods and the corresponding R package implementing them are as follow:

|Method Name|R Package|
| -------------------- | --------------------------------|
| Arima | forecast |
| Arima (force seasonality) | forecast |
| Theta | forecast, forecastHybrid |
| ETS | forecast |
| ETS (force damped) | forecast |
| Bagged ETS | forecast |
| STL | forecast |
| Neural Network (nnetar)| forecast |
| Hybrid | forecastHybrid|
| Prophet | prohpet|


## Dependencies

The script depends on the following packages:

 - forecast
 - devtools
 - elastic
 - forecastHybrid
 - prophet

These required packages should be installed beforehand using the following commands:

     install.packages("devtools")
     library(devtools)
     install_version("forecast", version="8.7")
     install_version("prophet", version="0.4")
     install_version("Rserve", version="1.7-3.1")
     install_version("elastic", version="0.8.4")
     install_version("forecastHybrid", version="4.2.17")
    
    
