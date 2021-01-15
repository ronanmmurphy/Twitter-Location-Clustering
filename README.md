# Twitter-Location-Clustering
K-Means Clustering to cluster given Twitter tweets by their geographic origins (coordinates), using Apache Spark RDDs.

Data used is twitter2D.txt1 with Twitter tweets and their attributes. The first two values in each line are the world coordinates from which the respective tweet was posted. 
The other values are a time stamp, a user id, an optional flag 1=spam/0=no spam, and finally the actual tweet message.
The coordinates are used to cluster the tweets into 4 groups. 
Output of the code is list of the tweets with their predicted clusters.
