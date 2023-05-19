# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions  as F
from pyspark.sql.types import IntegerType, StringType


# %%
scSpark = SparkSession.builder.appName("Assignment5.3").getOrCreate()

# %%
# Read data from a source and create a DataFrame
colunms = ["PassengerId","Survived","class","Name","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked","TimeStamp"]

titanic_df = scSpark.read.csv('./data/titanic.csv', sep=',', inferSchema=True, header=False)
titanic_df=titanic_df.toDF(*colunms)
titanic_df.show(5)

# %% [markdown]
# ### Task 1
# For numerical columns, calculate minimum, maximum and average values.
# 

# %%
# Colunms
allcols = [cols[0] for cols in titanic_df]
allcols

# Separating colunms
cat_cols = [item[0] for item in titanic_df.dtypes if item[1].startswith('string')]
numeric_cols = [col for col in titanic_df.columns if col not in cat_cols]

cat_df = titanic_df[cat_cols]
numeric_df = titanic_df[numeric_cols[:-1]].drop('Survived')

# Summary
numeric_df.describe().show()

# %% [markdown]
# ### Task 2 & 3
# For categorical columns, create and apply UDF that will change the last letter of every word to “1”,
# 
# And Sort DataFrame by the first column and save the results to the Parquet file.
# 
# 
# 

# %%
cols = ['Sex', 'Parch', 'class', 'Embarked']

def word_change(words): 
    words = str(words)
    if(len(words) == 1):
        return words[0:-1] + '1'
    return ' '.join([word[0:-1]+'1' for word in words.split()])

word_change_udf = F.udf(word_change, StringType())
for col in cols:
    titanic_df =  titanic_df.withColumn(col, word_change_udf(titanic_df[col]))
titanic_df.show(2)

final_df =  titanic_df.sort(titanic_df[0], asc=False)
final_df.write.mode("overwrite").save('./data/titanic.parquet')

# %%



