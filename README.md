<a name="readme-top"></a>
# 👨‍💻 Built with
<img src="https://www.gstatic.com/devrel-devsite/prod/v0e0f589edd85502a40d78d7d0825db8ea5ef3b99ab4070381ee86977c9168730/cloud/images/cloud-logo.svg" width="100" height="27.5" style="background-color:white"/>
<img src="https://img.shields.io/badge/Google Big Query-%234285F4?style=for-the-badge&logo=googlebigquery&logoColor=white"/>
<img src="https://www.devagroup.pl/blog/wp-content/uploads/2022/10/logo-Google-Looker-Studio.png" width="100" height="27,5"/>
<img src="https://airflow.apache.org/images/feature-image.png" width="100" height="27,5" />
<img src="https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue" />
<img src="https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white"/>
<img src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white"/>
<img src="https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white"/>
<img src="https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white"/>


<!-- ABOUT THE PROJECT -->
# ℹ️ About The Project

![gunb][gunb-url]

Project gathers information about issued building permissions in Poland. Information is collected from [GUNB](https://wyszukiwarka.gunb.gov.pl/) - General Office of Construction Supervision.<br>
Projects uses Apache Airflow which assures correct workflow. DAG is being run once per month and do the following:<br>
- downloads permissions .csv file and saves them on container
- validates the data
- cleanses the data
- uploads data to GoogleBigQuery database
- calculates the aggregate values
- sends email with validation report attached

Apart from the above there are visualizations prepared both in Jupyter Notebook and Google Looker.

# 📋 Apache Airflow DAG

DAG consists of 6 Tasks:<br>
- create table
- bulding_permission_download
- data-validation
- building_permission_to_gbq_upload
- calc_aggregates
- send_email

&shy;<img src="https://i.ibb.co/2YBRRpy/dag-screen-1.png"/>


# 🔍️ Google Looker

Dashboard is in Google Looker <br> 
&shy;<img src="https://raw.githubusercontent.com/PKuziola/gunb_permissions/main/img/google_looker_gif.gif"/>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 🗺️ Jupyter Notebook

Jupyter Notebook contrary to Google Looker connects to POSTGRESQL database<br>
There is data analysis with use of libraries such as pandas, matplotlib, seaborn and numpy<br>
In order to prepare map geopandas library was used, polygons where obtained from .shp file <br>

&shy;<img src="https://i.ibb.co/4tMxkZZ/mapkageopandas.png"/>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 🛢Database Details

Database is on GoogleBigQuery and dataset consists of two tables

### permissions

| Column_name  | Datatype | 
| ------------- | ------------- |
| numer_ewidencyjny_system  | STRING   
| numer_ewidencyjny_urzad  | STRING  |
| data_wplywu_wniosku_do_urzedu  | TIMESTAMP  | 
| nazwa_organu  | STRING  |
| wojewodztwo_objekt  | STRING  | 
| objekt_kod_pocztowy  | STRING  | 
| miasto  | STRING  | 
| terc  | STRING  |
| cecha  | STRING  |
| cecha_1  | STRING  | 
| ulica  | STRING  |
| ulica_dalej  | STRING  |
| nr_domu  | STRING  | 
| kategoria  | STRING  | 
| nazwa_zam_budowlanego  | STRING  |
| rodzaj_zam_budowlanego  | STRING  |
| kubatura  | FLOAT  | 
| stan  | STRING  | 
| jednostki_numer  | STRING  |
| obreb_numer  | FLOAT  |
| numer_dzialki  | STRING  |
| numer_arkusza_dzialki  | STRING  | 
| nazwisko_projektanta  | STRING  |
| imie_projektanta  | STRING  |
| projektant_numer_uprawnien | STRING  | 
| projektant_pozostali  | STRING  | 

### permissions_aggregates

| Column_name  | Datatype |
| ------------- | ------------- |
| data_wplywu_wniosku_do_urzedu  | STRING  |
| terc  | STRING  |
| **  | INTEGER  |
** - counts of every possible combination of type and measure for last 1, 2, 3 months

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 📧 E-mail
&shy;<img src="https://i.ibb.co/Y29LyN6/email.png"/>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# ✔️ Validation Report

Report prepared with great expectations module.  <br>
Value constraints are: <br>
- <strong>Table-Level Expectations</strong> -> Data must have greater than or 1000 rows <br>
- <strong>data_wplywu_wniosku_do_urzedu</strong> -> Values must not be null, at least 95 % of the time <br>
- <strong>kategoria</strong> -> Values must belong to particular set of values <br>
- <strong>numer_ewidencyjny_system</strong> -> Values must not be null, at least 95 % of the time <br>
- <strong>obiekt_kod_pocztowy</strong> -> Values must match this regular expression: ^\d{2}-\d{3}$, at least 95 % of the time<br>
- <strong>rodzaj_zam_budowlanego_new</strong> -> Values must belong to particular set of values <br>
- <strong>terc</strong> -> Values must not be null, at least 95 % of the time <br>
- <strong>wojewodztwo_objekt</strong> -> Values must belong to particular set of values <br>


&shy;<img src="https://i.ibb.co/7Cd0fn6/validation.png"/>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 🔑Setup

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Getting Started zmien

```bash
# Clone the repository
$ git clone https://github.com/PKuziola/gunb_permissions.git
# Navigate to the project folder
$ cd gunb_permissions
# Remove the original remote repository
$ git remote remove origin
```
### Necessary adjustments

To connect with Google Cloud Platform you need to download .json key and put in in directory, also it is advised to rename project,dataset and table id's in code<br>
In docker-compose.yaml replace project_id in AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT variable

### Setuping local variables

Before running project you need to assign environmental variables .env file.

```bash
DB_PASSWORD=        
EMAIL_ADDRESS=      
SMTP_PASSWORD= 
GOOGLE_CLOUD_JSON_KEY=
```
DB_PASSWORD - POSTGRES Database Password<br>
SMTP_PASSWORD - less secure apps google generated password


# ⚙️ Run locally

Build postgres database container
```bash
cd database
docker build -t postgres_db .
docker run -it --name postgres_container postgres_db
cd ..
```

Build extended apache airflow image
```bash
docker build -f extended_dockerfile_image_files/Dockerfile . --tag extending_airflow:latest
```
Build apache-airflow container
```bash
docker compose up
```
Open apache-airflow in web browser
```bash
localhost:8080
```

If you want to download neccessary geospatial files do below commands or use download_shp_files.py
```bash
curl -O https://www.gis-support.pl/downloads/2022/wojewodztwa.zip
curl -O https://www.gis-support.pl/downloads/2022/powiaty.zip
```


<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 🌲 Project tree
```bash
.
├───dags
│   └── permissions_dag.py
├───database
│   └── Dockerfile
├───img
│   └── google_looker.gif
├───extended_dockerfile_image_files
│   ├── Dockerfile
│   └── requirements.txt
├───local_development
│   ├── download_shp_files.py
│   └── polygon_upload.ipynb
├── .env - sample  
├── README.md 
├── data_analysis.ipynb
├── docker-compose.yaml
├── license.txt
└── requirements.txt


```
<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
# 📄 License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


[gunb]: https://wyszukiwarka.gunb.gov.pl/
[gunb-url]: https://i.ibb.co/LQJBLKF/gunb.png
