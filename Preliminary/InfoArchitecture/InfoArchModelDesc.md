**Source** is the starting point, which is the data source **Bus Breakdown and Delays Link:** https://data.cityofnewyork.us/Transportation/Bus-Breakdown-and-Delays/ez4e-fazm.

We **gather** the data in increments, having each instance put into **temporary storage**. After an instance completed the following steps, the temporary storage is reset.

**Extract** the data from the temporary storage for it to be **cleaned** with only proper values, **reformatted** into correct data types and dates to be in the proper format, and **transformed** for code to use the data efficiently.

**Consolidate** ensures the values that were transformed meet the requirements of the data reference table.

**Loading** prepares the data and puts it into the data warehouse.

**Data Warehouse** allows use of the dimensional model to execute queries effectively. All instances of data are stored here.