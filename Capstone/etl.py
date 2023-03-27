import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import split, upper, col


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"


def data_qual_check(dataframe, table_name):
    """
    Checks to make sure tables are not empty and displays table schema
    """
    if dataframe.count() == 0:
        print(table_name + " table is empty")
    else:
        print("Total records in " + table_name + " table: ")
        print(dataframe.count())
        print()
    
    print("Schema for " + table_name + " table: ")
    dataframe.printSchema()

def create_spark_session():
    """
    This function creates a spark session and returns it to be used in the rest of the etl.py script.
    """
    spark = SparkSession.builder\
                        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
                        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
                        .enableHiveSupport().getOrCreate()
    return spark

def process_immigration_data(spark):
    """
    Takes immigration data and makes it more readable and usable.
    Dates are converted to readable dates and string representations
    of integers are converted to integers.
    """
    df_spark =spark.read.load('./sas_data')
    
    df_spark.createOrReplaceTempView("imm_table_1")
    
    immigration_df = spark.sql('SELECT cicid AS cicid, \
                          i94yr AS year, \
                          i94mon AS month, \
                          i94cit AS country_code1, \
                          i94res AS country_code2, \
                          i94port AS port_of_entry, \
                          date_add("1960-01-01", arrdate) AS arrival_date, \
                          i94mode AS mode_of_arrival, \
                          i94addr AS state_or_terr, \
                          date_add("1960-01-01", depdate) AS departure_date, \
                          i94bir AS age, \
                          i94visa AS VISA, \
                          occup AS occupation, \
                          entdepa AS arrival_flag, \
                          entdepd AS departure_flag, \
                          entdepu AS update_flag, \
                          matflag AS match_flag, \
                          biryear AS birth_year, \
                          to_date(dtaddto, "MMddyyy") AS date_added, \
                          gender AS gender, \
                          insnum AS ins_number, \
                          airline AS incoming_airline, \
                          admnum AS admission_number, \
                          fltno AS flight_number, \
                          visatype AS visa_type \
                          FROM imm_table_1')
    
    imm_df = immigration_df.withColumn('country_code1', immigration_df.country_code1.cast(IntegerType()))\
                       .withColumn('country_code2', immigration_df.country_code2.cast(IntegerType()))\
                       .withColumn('cicid', immigration_df.cicid.cast(IntegerType()))\
                       .withColumn('year', immigration_df.year.cast(IntegerType()))\
                       .withColumn('month', immigration_df.month.cast(IntegerType()))\
                       .withColumn('mode_of_arrival', immigration_df.mode_of_arrival.cast(IntegerType()))\
                       .withColumn('age', immigration_df.age.cast(IntegerType()))\
                       .withColumn('VISA', immigration_df.VISA.cast(IntegerType()))\
                       .withColumn('birth_year', immigration_df.birth_year.cast(IntegerType()))
    
    imm_df_final = imm_df.drop('occupation', 'update_flag', 'ins_number')
    
    data_qual_check(imm_df_final, 'immigration')
    
    imm_df_final.write.mode("overwrite").parquet(path="./immigration_table")
    
def get_label_description_data():
    """
    Reads the label description file and returns its content to be used
    for other dataframes.
    """
    file = open('./I94_SAS_Labels_Descriptions.SAS', 'r')
    content = file.readlines()
    
    return content

def process_country_codes(spark, content):
    """
    Takes lines from label description file and creates dataframe for country
    codes.
    """
    lines = []
    
    for i in range(9, 298):
        lines.append(content[i])
    
    my_data = []
    for i in range(len(lines)):
        my_data.append(lines[i].split('='))
        
    for i in range(len(my_data)):
        my_data[i] = [my_data[i][0].strip(), my_data[i][1].strip().strip('\'')]
        
    columns = ['country_code', 'country']
    
    schema = StructType([\
                    StructField('country_code', StringType()),
                    StructField('country', StringType())])
    
    country_codes_df = spark.createDataFrame(data = my_data, schema = schema)
    
    country_df_final = country_codes_df.withColumn('country_code', country_codes_df.country_code.cast(IntegerType()))
    
    data_qual_check(country_df_final, 'country')
    
    country_df_final.write.mode("overwrite").parquet(path="./country_table")
    
def process_us_port_data(spark, content):
    """
    Takes lines from label description file and creates dataframe for US port codes.
    """
    port_lines = []
    
    for i in range(302, 963):
        port_lines.append(content[i])
    
    my_data2 = []
    
    for i in range(0, 516):
        my_data2.append(port_lines[i].split('='))
    
    # Fixing typos and abnormalities
    my_data2[28][1] = my_data2[28][1].strip().strip('\'').strip()
    my_data2[28][1] = my_data2[28][1].replace(' ', ', ')
    my_data2[76][1] = 'WASHINGTON DC, DC'
    my_data2[49][1] = 'PACIFIC COAST HWY STATION, CA'
    my_data2[217][1] = 'WARROAD INTL SPB, MN'
    my_data2[417][1] = 'HULL FIELD SUGAR LAND ARPT, TX'
    my_data2[385][1] = 'BLACK HILLS SPEARFISH, SD'
    my_data2[387][1] = 'SAIPAN, NORTHERN MARIANA ISLANDS'
    my_data2[428][1] = 'PASO DEL NORTE, TX'
    my_data2[444][1] = 'CRUZ BAY ST JOHN, VI'
    my_data2[478][1] = 'BELLINGHAM, WA #INTL'
    my_data2.append(port_lines[577].split('='))
    for i in range(579, 591):
        my_data2.append(port_lines[i].split('='))
        
    for i in range(len(my_data2)):
        my_data2[i] = [my_data2[i][0].strip().strip('\''), my_data2[i][1].strip().strip('\'').strip().split(', ')[0], my_data2[i][1].strip().strip('\'').strip().split(', ')[1]]
    
    for row in my_data2:
        if (len(row[2]) == 2 or row[2] == 'NORTHERN MARIANA ISLANDS'):
            row.append(None)
        else:
            last_col = row[2].split(' ', 1)
            row[2] = last_col[0]
            row.append(last_col[1])
    
    us_port_schema = StructType([\
                    StructField('port_code', StringType()),
                    StructField('port_name', StringType()),
                    StructField('state_code', StringType()),
                    StructField('extra_info', StringType())])

    us_port_df = spark.createDataFrame(data = my_data2, schema = us_port_schema)
    
    data_qual_check(us_port_df, 'us_port')
    
    us_port_df.write.mode("overwrite").parquet(path="./us_port_table")


def process_state_data(spark, content):
    """
    Takes lines from label description file and creates dataframe for state codes.
    """
    state_lines = []
    
    for i in range(981, 1036):
        state_lines.append(content[i])
    
    my_data3 = []
    for line in state_lines:
        split_data = line.split('=')
        my_data3.append([split_data[0].strip().strip('\''), split_data[1].strip().strip('\'')])
        
    state_schema = StructType([\
                          StructField('state_code', StringType()),
                          StructField('state_name', StringType())])
    
    state_df_final = spark.createDataFrame(data = my_data3, schema = state_schema)
    
    data_qual_check(state_df_final, 'state')
    
    state_df_final.write.mode("overwrite").parquet(path="./state_table")
    
def process_airport_data(spark):
    """
    Reads airport file and filters only US airports.  Creates dataframe and drops redundant columns.
    Converts string representation of integers to integers.
    """
    airport_df = spark.read.options(delimiter = ',').option("header", True).csv('airport-codes_csv.csv')
    airport_df = airport_df.filter(airport_df.iso_country == 'US')

    airport_df_final = airport_df.withColumn('state_code', split(airport_df.iso_region, '-')[1])\
                                 .withColumn('elevation_ft', airport_df.elevation_ft.cast(IntegerType()))\
                                 .drop('iso_region')\
                                 .drop('continent')\
                                 .drop('iso_country')

    data_qual_check(airport_df_final, 'airport')
    
    airport_df_final.write.mode("overwrite").parquet(path="./airport_table")
    
def process_city_data(spark):
    """
    Reads US city demographic information and converts string representations of integers to integers. Converts
    string representations of doubles to doubles.
    """
    us_cities_demog_df = spark.read.options(delimiter = ';').option("header", True).csv('us-cities-demographics.csv')
    
    city_dem_df = us_cities_demog_df.withColumn('Median_Age', us_cities_demog_df['Median Age'].cast(DoubleType()))\
                                    .withColumn('City', upper(col('City')))\
                                    .withColumn('State', upper(col('State')))\
                                    .withColumn('Male_Population', us_cities_demog_df['Male Population'].cast(IntegerType()))\
                                    .withColumn('Female_Population', us_cities_demog_df['Female Population'].cast(IntegerType()))\
                                    .withColumn('Total_Population', us_cities_demog_df['Total Population'].cast(IntegerType()))\
                                    .withColumn('Number_of_Veterans', us_cities_demog_df['Number of Veterans'].cast(IntegerType()))\
                                    .withColumn('Foreign-born', us_cities_demog_df['Foreign-born'].cast(IntegerType()))\
                                    .withColumn('Average_Household_Size', us_cities_demog_df['Average Household Size'].cast(DoubleType()))\
                                    .withColumn('Count', us_cities_demog_df['Count'].cast(IntegerType()))\
                                    .drop('Median Age')\
                                    .drop('Male Population')\
                                    .drop('Female Population')\
                                    .drop('Total Population')\
                                    .drop('Number of Veterans')\
                                    .drop('Average Household Size')\
                                    .drop('State Code')

    data_qual_check(city_dem_df, 'city')
    
    city_dem_df.write.mode("overwrite").parquet(path="./city_dem_table")
    
def main():
    """
    Main function to run all other processes.
    """
    spark = create_spark_session()
    
    process_immigration_data(spark)
    
    content = get_label_description_data()
    
    process_country_codes(spark, content)
    
    process_us_port_data(spark, content)
    
    process_state_data(spark, content)
    
    process_airport_data(spark)
    
    process_city_data(spark)

    spark.stop()

if __name__ == "__main__":
    main()
