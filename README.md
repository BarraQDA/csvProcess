# csvProcess

These Python scripts for manipulating [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) files were originally written as part of [twitterScrape](https://github.com/BarraQDA/twitterScrape). Since they have more general application I have moved them to their own library.

## Features

- These scripts use Python´s multiprocessing library to use multiple threads to run faster
- Like all the scripts in [twitterScrape](https://github.com/BarraQDA/twitterScrape), these scripts create self-documenting CSV files, which can be automatically re-generated as required.

## The scripts

### csvProcess.py

The original script, [csvProcess.py](csvProcess.py) reads a CSV file line by line, outputting zero or one line into a new CSV file. It can perform the following functions:

- Filter the CSV line using either a regular expression, Python code or a date field
- Calculate new columns using either a regular expression or Python

### csvCollect.py

[csvCollect.py](csvCollect.py) summarises a CSV file by extracting information from each row using a regular expression, and counting the number of occurrences of each value of that information. It can also calculate the number of occurrences of each value within a given time period.