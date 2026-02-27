# MongoDB Atlas setup

This workshop requires a MongoDB Atlas account and a cluster with a sample dataset.

MongoDB Atlas free tier is sufficient for the scope of the workshop. 
Also, a single MongoDB cluster can be shared by multiple workshop attendees, as it is used as read-only source.

### Step by step

These are the steps to create a new, free MongoDB Atlas account, create a MongoDB cluster with sample data, and enable access to the cluster from Confluent Cloud Flink.

1. Create account, set 2PA
2. Create a Cluster
    - Free tier
    - AWS, same region as workshop
    - Preload sample dataset
3. Create a user
4. Edit Network Access > IP List > Add IP Address : Allow Access from Anywhere
5. Find the cluster endpoint:
    1. Clusters > Connect > Drivers 
    2. Check out the connection string, something like `mongodb+srv://<db-user>:<db_password>@my-cluster.something.mongodb.net/?appName=My-app`: the endpoint for CC Flink is the connection string without the db-user and db-password, from the beginning up to and including the trailing `/`. For example: `mongodb+srv://my-cluster.something.mongodb.net/` 


Take note of the following information, you will need them in the workshop:
* db-user
* db-password
* cluster endpoint