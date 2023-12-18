# YouTube-Data-Harvesting-
YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit
The main.py python code fetchs data from the YouTube using its API, stores it in a MongoDB datalake and moves to respective tables in MySQL database. It further uses the Streamlit library as frontend for fetching data from MySQL and displaying quried data. This project is part of curriculam requirement by GUVI_IITM Machine Learning Course.
1. The code after impoting required packages interacts with You Tube API to get the required data from respective You Tube resources. The data harvested included data about Channel, Playlist, Videos and Comments.
2. The collected data is then pushed into Mongo DB Datalake as documents
3. Respective data tables are created in MySQL DB and the data from documents are then moved to respective tables. Data/datatype massaging is done as required.
4. Streamlit aplplication is used to display the channel data and quried data in the front end. 
