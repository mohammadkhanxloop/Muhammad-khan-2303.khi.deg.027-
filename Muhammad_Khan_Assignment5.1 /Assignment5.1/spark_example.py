# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions  as F
from pyspark.sql.types import IntegerType

# %%
scSpark = SparkSession.builder.appName("Spark Example").getOrCreate()

# %%
# Load Data
transaction_df = scSpark.read.csv("./data/store_transactions/*.csv", header=True)
customer_df= scSpark.read.csv("./data/customers.csv", header=True)
product_df= scSpark.read.csv("./data/products.csv", header=True)


# %%
transaction_df.show(2)
customer_df.show(2)
product_df.show(2)

# %% [markdown]
# ### Task 1
# What are the daily total sales for the store with id 1?
# 

# %%
def store_1_daily_sales():
    store_1_df = transaction_df.filter(transaction_df['StoreId'] == 1)
    store_1_df = store_1_df.join(product_df, store_1_df['ProductId'] == product_df['ProductId'], 'inner')

    daily_sales = store_1_df.withColumn(
        "TotalSales", (store_1_df["Quantity"] * store_1_df["UnitPrice"]))

    return  daily_sales.select(F.sum(daily_sales["TotalSales"])).first()[0]

print(f'Daily total sales for the store with id 1: {store_1_daily_sales()}')

# %% [markdown]
# ### Task 2
# What are the mean sales for the store with id 2?

# %%
def store_2_mean_sales():
    store_2_df = transaction_df.filter(transaction_df['StoreId'] == 2)
    store_2_df = store_2_df.join(product_df, store_2_df['ProductId'] == product_df['ProductId'], 'inner')

    store2_daily_sales = store_2_df.withColumn(
        "TotalSales", (store_2_df["Quantity"] * store_2_df["UnitPrice"]))

    return store2_daily_sales.agg({'TotalSales': 'mean'}).first()[0]

print(f'Mean sales for the store with id 2: {store_2_mean_sales()}')



# %% [markdown]
# ### Task 3
# What is the email of the client who spent the most when summing up purchases from all of the stores?

# %%
def client_max_purchase():
    client_trans = transaction_df.join(customer_df, transaction_df['CustomerId'] == customer_df['CustomerId'], 'inner')
    client_id = client_trans.join(product_df, client_trans['ProductId'] == product_df['ProductId'])
    client_daily_sales = client_id.withColumn(
        "TotalSales", (client_id["Quantity"] * client_id["UnitPrice"]))
    
    return client_daily_sales.groupBy('Email').agg(F.sum(F.col('TotalSales')).alias('sum')).orderBy(F.desc('sum')).first()[0]
    
print(f'Email of the client who spent the most when summing up purchases from all of the stores: {client_max_purchase()}')

# %% [markdown]
# ### Task 4
#  Which 5 products are most frequently bought across all stores?
# 

# %%
def top__5_products():
    products_store = transaction_df.join(product_df, transaction_df['ProductId'] == product_df['ProductId'], 'inner')
    return products_store.groupBy('Name').agg(F.sum(F.col('Quantity').cast(IntegerType())).alias('TotalQuantitiyPurchased')).orderBy(F.desc('TotalQuantitiyPurchased')).show(5)

top__5_products()


