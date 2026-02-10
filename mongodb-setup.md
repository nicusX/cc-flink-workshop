# MongoDB Atlas setup

This workshop requires a MongoDB Atlas account, and a cluster with sample dataset.

MongoDB Atlas free tier is sufficient for the scope of the workshop. 
Also, a single MongoDB cluster can be shared by multiple workshop attendees, as it is used as read-only source.

**TODO improve instructions**

1. Create account, set 2PA
2. Create a Cluster
    - Free tier
    - AWS, same region as workshop
    - Preload sample dataset
3. Create a user
4. Edit Network Access > IP List > Add IP Address : Allow Access from Anywhere
5. Find the cluster endpoint:
    1. Clusters > Connect > Drivers 
    2. Check out the connection string, something like `mongodb+srv://<db-user>:<db_password>@my-cluster.something.mongodb.net/?appName=My-app`: the endpoint for CC Flink is the string omitting db-user and db-password up to the trailing `/`, e.g. `mongodb+srv://my-cluster.something.mongodb.net/` 


Take note of the following:
* db-user
* db-password
* cluster endpoint