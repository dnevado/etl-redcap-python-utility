<a name="readme-top"></a>

[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]


<!-- PROJECT LOGO -->
<br />
<div align="center">
  

  <h3 align="center">REDCAP ETL TRANSFORMATION </h3>

  <p align="center">
    Python utility to help you extract data from Excel files and load into a predefined list field for a specific REDCAP Project
    <br />    
    ·
    <a href="https://github.com/dnevado/etl-redcap-python-utility/issues">Report Bug</a>
    ·
    <a href="https://github.com/dnevado/etl-redcap-python-utility/issues">Request Feature</a>
  </p>
</div>



<!-- ABOUT THE PROJECT -->
## About The Project

[![Product Name Screen Shot][product-screenshot]](https://github.com/dnevado/etl-redcap-python-utility/issues)

Data transformation from your base data is executed by this utility, a simple Python file which gets 2 files as parameters, first, the data file with the whole database , lets say "SampleData.xlsx" and the template file in csv downloaded from your project field settings.  In order to get it ready to be uploaded , the template's next row will be filled with those mapping field names from the initial dataset in Excel.

Then,

* Second row is supposed to have  field names to establish the relationship
* Some basic formulas are supported (ex, "**if subcont_biv=1,1**")

### Built With

Pandas & Numpy libraries are supported 

