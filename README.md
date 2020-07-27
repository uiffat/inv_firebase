# NGT Investment Fund Data Analysis [WIP]

This project analyses an investment fund dataset, comprising of several subfunds and share classes, called instruments.
The dataset is a panel data, with several time series for each instrument, over a common time period.
 
The analysis includes several data quality checks and calculation of the correlation between instruments.

Data quality checks performed -
All data quality checks have been performed at a per instrument level-
* Count number of data points for each instrument
* Mean NAV per share value for each instrument
* Count of the number of valuation days missing (using holiday Calendar of Luxembourg)
* Currency(s) used for each instrument 

Instrument correlation is performed using the Pearson method, requiring at least 50% of the data in the time period to be available.
It is observed that the NAV value time series is stationary over the time period present in the dataset, therefore correlation values 
computed is not spurious.
 
### Code Components

**1. InstrumentDataAnalysis.py** :
Defines an investment fund data class. Includes all related methods to analyse and work with the data

**2. firebase_connect.py** :
Uses the firebase-admin module to connect to a firebase realtime database and provides simple functions to do connect and write.

**3. main.py** :
Main module works with the provided data and writes the result to the database using the above two modules
