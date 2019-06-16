# Popular-movies
A spark program to find the most popular movie based on the number of views and display the movie name instead of the movie ID

The dataset used (ml-00k) contains 100k movie ratings- the movie IDs are in u.data file and movie names corresponding to each movie ID is in u.item file.

* popular-movies.py 
    - contains python code to find most popular movies using RDDs 
* popular-movies-sql.py
    - contains python code for getting most popular movies by using dataframes and SQL style functions on dataframes instead of RDDs (SparkSQL)
