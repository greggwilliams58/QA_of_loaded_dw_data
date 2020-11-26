# Data-Validation
 This was written to perform a series of QA checks on data which had been loaded into the Data Warehouse.  The aim was to check the latest load of data against the prevously loaded dataset and check for variances as well as output percentage changes between the two loads and other summary data.
 
 # Installation
 This was written in visual studio, so VS will be needed to open the .sln file.
 
 # Useage
 This will not work outside of ORR's secure VPN as the code uses SQLAlchemy to access previously loaded data from the ORR data warehouse.
 
 Execute the DataValidation.py file and the code will loop through the datasets listed in the `unique_feed_features` dictionary.
 

