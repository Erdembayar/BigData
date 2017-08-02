1.Write a basic MapReduce java program without combiner or in-mapper combining to calculate the average temperature per year.

2.Write a MapReduce java program with in mapper combining design pattern to calculate the average temperature per year.

Submit the Java files, class files and output file for both the above problems.


For both these problems, use the attached sample of NCDC weather dataset.  

Format of this National Climatic Data Center (NCDC) record is as follows:

0057
332130                 # USAF weather station identifier
99999                   # WBAN weather station identifier
19500101           # observation date
0300                     # observation time
4
+51317                 # latitude (degrees x 1000)
+028783              # longitude (degrees x 1000)
FM-12
+0171                   # elevation (meters)
99999
V020
320                        # wind direction (degrees)
1                            # quality code
N 0072
1 00450                #sky ceiling height (meters)
1                            # quality code
C
N 010000             #visibility distance (meters)
1                            # quality code
N
9
-0128                   # air temperature (degrees Celsius x 10)
1                            # quality code
-0139                    # dew point temperature (degrees Celsius x 10)
1                            # quality code
10268                   # atmospheric pressure (hectopascals x 10)
1                            # quality code
10268                   # atmospheric pressure (hectopascals x 10)
1                            # quality code