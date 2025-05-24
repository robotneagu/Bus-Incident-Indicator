# Bus-Incident-Indicator

Credit to NYC Open Data for creating an open dataset so large it can used in a project like this.
Credit to Professor Jefferson Bien-Aime for helping me thoughout the project.

**Note:** All Python scripts used require the user to have Azure CLI installed on their machine. This is used to execute the scripts.

### Project Goals
This project aims at using the data collected to predict and provide BI solutions on bus incidents within the United States.

Currently, we are looking at an application-like product where the user can do the following:
  Type a code to get information on a specific incident<br>
  Have a live BI interface of incidents<br>
  Provide predictive information on why the bus is late and what the cause could be.<br>

The project is divided into two sections: a planning phase and technical phase.

The planning phase is meant to ensure we have all aspects done correctly to model a proper data warehouse.
The technical phase will construct the data warehouse and perform the operations of the project goals.

### Data Requirements

Based on the business and functional requirements, we need our data to contain bus incident information such as: 
  * Where and how it occurred
  * How many students were affected
  * Weather during the incident
  * Backend info such as whether the bus driver contacted OPT.

#### Data Source
**Bus Breakdown and Delays Link:** https://data.cityofnewyork.us/Transportation/Bus-Breakdown-and-Delays/ez4e-fazm
Data was accessed using the API Endpoint.

Information Architecture, Data Architecture, and Dimensional Modeling can be seen in the Preliminary folder.

### Data Model

Accessible via the Data References folder. It displays a star schema similar to the dimensional model. However, the variables have been updated based on the data warehouse.

### Data Warehouse

The fact and dimensions have been included as an Excel file in the Preliminary folder.

Snowflake was used to create the data warehouse. SQL scripts were used to extract the data from Azure and store them on the tables in Snowflake. 

Snowflake requires the user to create a database and a stage, where the stage connects Azure. It is recommended to use the SaS method for security reasons.

### Storage

Data will be stored using Microsoft Azure. A Python script is included on how to store the data as a CSV file, or a blob on this platform.

Snowflake also stored the data once it was retrieved from the blobs.