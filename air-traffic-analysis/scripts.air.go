CREATE TABLE IF NOT EXISTS airtrafic.airtraffic_table (
    activity_period int,
    operating_airline text,
    operating_airline_IATA_code text,
    published_airline text,
    published_airline_IATA_code text,
    GEO_summary text,
    GEO_region text,
    activity_type_code text,
    price_category_code text,
    terminal text,
    boarding_area text,
    passenger_count int,
    adjusted_activity_type_code text,
    adjusted_passenger_count int,
    year int,
    month text,
    PRIMARY KEY(activity_period, operating_airline)
)WITH CLUSTERING ORDER BY (operating_airline ASC);

df_airport = df_airport.withColumnRenamed("Activity Period", "ActivityPeriod")
.withColumnRenamed("Activity Period", "ActivityPeriod")
.withColumnRenamed("Operating Airline", "OperatinAirline")
.withColumnRenamed("Operating Airline IATA Code", "OperatingAirlineIATACode")
.withColumnRenamed("Published Airline", "PublishedAirline")
.withColumnRenamed("Published Airline IATA Code", "PublishedAirlineIATACode")
.withColumnRenamed("GEO Summary", "GEOSummary")
.withColumnRenamed("GEO Region", "GEORegion")
.withColumnRenamed("Activity Type Code", "ActivityTypeCode")
.withColumnRenamed("Price Category Code", "PriceCategoryCode")
.withColumnRenamed("Boarding Area", "BoardingArea")
.withColumnRenamed("Passenger Count", "PassengerCount")
.withColumnRenamed("Adjusted Activity Type Code", "AdjustedActivityTypeCode")
.withColumnRenamed("Adjusted Passenger Count", "Adjusted Passenger Count")

DOWNLOAD DSBUlk
curl -OL https://downloads.datastax.com/dsbulk/dsbulk-1.9.0.tar.gz
tar -xzvf dsbulk-1.9.0.tar.gz
dsbulk-1.9.0/bin/dsbulk --version

LOAD DATA
dsbulk load --connector.name csv --connector.csv.url Air_Traffic_Passenger_Statistics.csv -k airtrafic -t airtraffic_table -b "secure-connect-airtrafficproyect.zip" -u stKCxJURhGhCMIjKHZYKFTLE -p hJbpsZq6tGOk0-KRl4IjPxM2Z98N-MIXAz.O..GRY3vcn+uMYeyt_WbNZYZ3HA-j75_v7J.b7vhXDXeO.WR3INmnN3cH_ZARa7xp,9oj,THzAMnOs+fYY29y7RND,2RN -header false -delim "," --schema.allowMissingFields true -m "0=activity_period, 1=operating_airline, 2=operating_airline_iata_code, 3=published_airline , 4=published_airline_iata_code, 5=geo_summary ,6=geo_region, 7=activity_type_code, 8=price_category_code, 9=terminal , 10=boarding_area , 11=passenger_count,12=adjusted_activity_type_code, 13=adjusted_passenger_count, 14=year, 15=month"

QUERY - UNLOAD CHINA
token@cqlsh:airtrafic> select * from airtrafic.airtraffic_table WHERE Operating_Airline = 'Air China' ALLOW FILTERING;
dsbulk unload -url airchina_data.csv -query "select activity_period ,operating_airline ,operating_airline_IATA_code ,published_airline ,published_airline_IATA_code ,GEO_summary ,GEO_region ,activity_type_code ,price_category_code ,terminal ,boarding_area ,passenger_count ,adjusted_activity_type_code,year,month  from airtrafic.airtraffic_table WHERE Operating_Airline = 'Air China' ALLOW FILTERING" -b "secure-connect-airtrafficproyect.zip" -u stKCxJURhGhCMIjKHZYKFTLE -p hJbpsZq6tGOk0-KRl4IjPxM2Z98N-MIXAz.O..GRY3vcn+uMYeyt_WbNZYZ3HA-j75_v7J.b7vhXDXeO.WR3INmnN3cH_ZARa7xp,9oj,THzAMnOs+fYY29y7RND,2RN

QUERY - UNLOAD AIR BERLIN
token@cqlsh:airtrafic> select * from airtrafic.airtraffic_table WHERE Operating_Airline = 'Air Berlin' AND boarding_area = 'G' ALLOW FILTERING;
dsbulk unload -url airberlin_data.csv -query "select activity_period ,operating_airline ,operating_airline_IATA_code ,published_airline ,published_airline_IATA_code ,GEO_summary ,GEO_region ,activity_type_code ,price_category_code ,terminal ,boarding_area ,passenger_count ,adjusted_activity_type_code,year,month from airtrafic.airtraffic_table WHERE Operating_Airline = 'Air Berlin' AND boarding_area = 'G' ALLOW FILTERING" -b "secure-connect-airtrafficproyect.zip" -u stKCxJURhGhCMIjKHZYKFTLE -p hJbpsZq6tGOk0-KRl4IjPxM2Z98N-MIXAz.O..GRY3vcn+uMYeyt_WbNZYZ3HA-j75_v7J.b7vhXDXeO.WR3INmnN3cH_ZARa7xp,9oj,THzAMnOs+fYY29y7RND,2RN
----------------------------------------
PYSPARK
LOAD DATA IN DATAFRAME
from pyspark.sql.functions import col
df_airport = spark.read.options(inferSchema='True',delimiter=',', header=True).csv("/content/drive/MyDrive/TOKIO/Big Data - Cloud Computing/01 - Big Data/PROYECTO FINAL/Air_Traffic_Passenger_Statistics.csv")

TRIM SPACES IN COLUMNS
df_airport = df_airport.withColumnRenamed("Activity Period", "ActivityPeriod") \
.withColumnRenamed("Activity Period", "ActivityPeriod") \
.withColumnRenamed("Operating Airline", "OperatinAirline") \
.withColumnRenamed("Operating Airline IATA Code", "OperatingAirlineIATACode") \
.withColumnRenamed("Published Airline", "PublishedAirline") \
.withColumnRenamed("Published Airline IATA Code", "PublishedAirlineIATACode") \
.withColumnRenamed("GEO Summary", "GEOSummary") \
.withColumnRenamed("GEO Region", "GEORegion") \
.withColumnRenamed("Activity Type Code", "ActivityTypeCode") \
.withColumnRenamed("Price Category Code", "PriceCategoryCode") \
.withColumnRenamed("Boarding Area", "BoardingArea") \
.withColumnRenamed("Passenger Count", "PassengerCount") \
.withColumnRenamed("Adjusted Activity Type Code", "AdjustedActivityTypeCode") \
.withColumnRenamed("Adjusted Passenger Count", "AdjustedPassengerCount")
df_airport.show(10)
df_airport.printSchema() 

COUNT DIFFERENT AIRLINES
df_airport.dropDuplicates(["OperatingAirline"]).select("OperatingAirline").count()
df_airport.dropDuplicates(["OperatingAirline"]).select("OperatingAirline").show(77)

AVG PASSENGER PER COMPANY
df_airport.groupBy("OperatingAirline").mean("PassengerCount","AdjustedPassengerCount").show()

DROP DUPLICATES BY GEORegion
df_GEORegion_no_duplicates = spark.sql("select * FROM " \
"df_airport_view a1, " \
"(SELECT GEORegion, MAX(PassengerCount) PassengerCount FROM df_airport_view GROUP BY GEORegion) a2 "\
"WHERE a1.GEORegion = a2.GEORegion " \
"AND a1.PassengerCount = a2.PassengerCount")
df_GEORegion_no_duplicates.show();

WRITE DROP DUPLICATES INTO csv
df_GEORegion_no_duplicates.write.options(header="True").csv("/content/drive/MyDrive/TOKIO/Big Data - Cloud Computing/01 - Big Data/PROYECTO FINAL/Entrega/Ficheros/airtraffic_drop_duplicates_georegion")

CORRELATION MATRIX
df_airport_pd.corr()
df_airport_pd.corr().style.background_gradient(cmap='coolwarm')

LINEAR REGRESSION
from pyspark.sql.functions import sum
df_pass_by_year = df_airport.groupBy("Year" , "Month").agg(sum("PassengerCount").alias("PassengerCountSum"))
df_pass_by_year.show()

--to pandas
df_pass_by_year_pd = df_pass_by_year.toPandas()

--convert calendar field
import calendar as cal

lower_ma = [m.lower() for m in cal.month_name]
df_pass_by_year_pd['Month'] = df_pass_by_year_pd['Month'].str.lower().map(lambda m: lower_ma.index(m)).astype('Int8')

df_pass_by_year_pd['Date'] = df_pass_by_year_pd[df_pass_by_year_pd.columns[0:2]].apply(lambda x: "-".join(x.values.astype(str)),axis="columns")
df_pass_by_year_pd['Date']= pd.to_datetime(df_pass_by_year_pd['Date']).dt.strftime("%Y-%m")
df_pass_by_year_pd.sort_values(by=["Date"])

--import linearmodel
from sklearn import linear_model

--dates to ordinal
import datetime as dt
df_pass_by_year_pd['Date']= pd.to_datetime(df_pass_by_year_pd['Date'])
df_pass_by_year_pd['DateOrd']=df_pass_by_year_pd['Date'].map(dt.datetime.toordinal)
df_pass_by_year_pd = df_pass_by_year_pd.sort_values(by=["DateOrd"])

--regression
model = linear_model.LinearRegression()

explicativas = df_pass_by_year_pd_simple[['DateOrd']] #independiente
objetivo = df_pass_by_year_pd_simple[['PassengerCountSum']] #dependiente



--prediction
pred = model.predict(X=df_pass_by_year_pd_simple[['DateOrd']])

df_pass_by_year_pd_simple.insert(3, 'Prediction' , pred)

pd.set_option('display.float_format', '{:.3f}'.format)
df_pass_by_year_pd_simple = df_pass_by_year_pd_simple.sort_values(by=["DateOrd"])
df_pass_by_year_pd_simple

--score
print(model.score(X=explicativas , y=objetivo))

--date to string AAAA-MM
df_pass_by_year_pd_simple['Date']= pd.to_datetime(df_pass_by_year_pd['Date']).dt.strftime("%Y-%m") #esto hace la columna un string