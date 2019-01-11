# StockCNN
Attempting to use a CNN to make daily high-low-close predictions from previous intra-day data.

This is a work in progress to scrape intra-day ticker data and use that as a continuous 1D-signal, and utilize a CNN to attempt to predict the following day's HLC.

**symbols_getter.py** scrapes all of the individual tickers.

**historical_scraper.py** scrapes the entire availble history of minute by minute data from all the the tickers from **symbols_getter.py**

**preprocess_json.py** parses out the json format from **historical_scraper.py** into a csv format.

**make_training.py** parses the csv data into a labeled training data.

**CNNreg.py** uses the training data to train a CNN, with the labeled previous day (390,1) array with the following day's HLC.

**predictions.py** uses the CNN to make predictions on the next available day in the data.
