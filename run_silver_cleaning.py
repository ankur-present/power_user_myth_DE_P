#!/usr/bin/env python
# coding: utf-8

# ## run_silver_cleaning
# 
# null

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#   COUNT(*) AS total_goal_rows,
#   COUNT(DISTINCT user_id) AS distinct_users_with_any_goal
# FROM bronze_goals


# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT count(*) AS total_users_rows
# from bronze_users


# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT count(*) as total_session_rows
# FROM bronze_sessions


# In[4]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT source, COUNT(*) AS row_count
# FROM bronze_goals
# GROUP BY source


# In[5]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT COUNT(DISTINCT user_id) AS users_with_real_goal
# FROM bronze_goals
# WHERE source = 'user'


# In[6]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT COUNT(*) AS onboarding_only_users
# FROM (
#   SELECT user_id
#   FROM bronze_goals
#   GROUP BY user_id
#   HAVING SUM(CASE WHEN source = 'user' THEN 1 ELSE 0 END) = 0
# )


# In[8]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT COUNT(*) AS multi_goal_users
# FROM (
#   SELECT user_id
#   FROM bronze_goals
#   WHERE source = 'user'
#   GROUP BY user_id
#   HAVING COUNT(*) > 1
# )


# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

users = spark.table("bronze_users")

silver_users = (
    users
    .withColumn(
        "platform",
        F.when(
            F.col("platform").isNull() |
            (F.trim(F.col("platform")) == ""),
            "unknown"
        )
        .otherwise(F.lower(F.trim(F.col("platform"))))
    )
    .withColumn(
        "country",
        F.when(
            F.col("country").isNull() |
            (F.trim(F.col("country")) == ""),
            "Unknown"
        )
        .otherwise(F.trim(F.col("country")))
    )
    .withColumn(
        "signup_date",
        F.to_date("signup_date")
    )
)

silver_users.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_users")


# In[2]:


display(spark.table("silver_users"))


# In[3]:


spark.sql("""
SELECT
    platform,
    COUNT(*) AS users
FROM silver_users
GROUP BY platform
ORDER BY users DESC
""").show()


# In[4]:


goals = spark.table("bronze_goals")
users = spark.table("silver_users")


# In[5]:


real_goals = (
    goals
    .filter(F.lower(F.trim(F.col("source"))) == "user")
    .withColumn(
        "created_at",
        F.to_timestamp("created_at")
    )
)


# In[6]:


goal_window = Window \
    .partitionBy("user_id") \
    .orderBy(F.col("created_at").asc())


# In[7]:


goal_window = Window \
    .partitionBy("user_id") \
    .orderBy(F.col("created_at").asc())


# In[8]:


first_real_goal = (
    real_goals
    .withColumn(
        "goal_rank",
        F.row_number().over(goal_window)
    )
    .filter(F.col("goal_rank") == 1)
    .drop("goal_rank")
)


# In[9]:


silver_goals = (
    first_real_goal
    .join(
        users.select("user_id", "signup_date"),
        on="user_id",
        how="left"
    )
    .withColumn(
        "days_since_signup",
        F.datediff(
            F.to_date("created_at"),
            F.col("signup_date")
        )
    )
    .select(
        "user_id",
        "created_at",
        "goal_type",
        "source",
        "signup_date",
        "days_since_signup"
    )
)


# In[10]:


silver_goals.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_goals")


# In[11]:


display(
    spark.table("silver_goals")
    .orderBy("user_id")
)


# In[12]:


spark.sql("""
SELECT COUNT(*) AS goal_creators
FROM silver_goals
""").show()


# In[13]:


sessions = spark.table("bronze_sessions")
users = spark.table("silver_users")


# In[14]:


clean_sessions = (
    sessions
    .withColumn(
        "session_ts",
        F.to_timestamp("session_ts")
    )
)


# In[15]:


silver_sessions = (
    clean_sessions
    .join(
        users.select("user_id", "signup_date"),
        on="user_id",
        how="left"
    )
    .withColumn(
        "days_since_signup",
        F.datediff(
            F.to_date("session_ts"),
            F.col("signup_date")
        )
    )
)


# In[16]:


silver_sessions.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_sessions")


# In[17]:


print("=== SILVER USERS ===")

spark.sql("""
SELECT
    COUNT(*) AS total_users,
    COUNT(DISTINCT user_id) AS distinct_users
FROM silver_users
""").show()


# In[18]:


print("=== SILVER GOALS ===")

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT user_id) AS distinct_users,
    MIN(days_since_signup) AS min_days,
    MAX(days_since_signup) AS max_days
FROM silver_goals
""").show()


# In[19]:


print("=== SILVER SESSIONS ===")

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT user_id) AS users,
    MIN(days_since_signup) AS min_days,
    MAX(days_since_signup) AS max_days
FROM silver_sessions
""").show()


# In[20]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT COUNT(*) AS sessions_before_signup
# FROM silver_sessions
# WHERE days_since_signup < 0


# In[21]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT COUNT(*) AS goals_before_signup
# FROM silver_goals
# WHERE days_since_signup < 0


# In[22]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# CREATE OR REPLACE TABLE gold_user_retention AS

# SELECT
#     u.user_id,
#     u.signup_date,
#     u.acquisition_channel,
#     u.platform,
#     u.country,

#     CASE
#         WHEN COUNT(s.user_id) > 0 THEN 1
#         ELSE 0
#     END AS retained_day_60

# FROM silver_users u

# LEFT JOIN silver_sessions s
#     ON u.user_id = s.user_id
#     AND s.days_since_signup = 60

# GROUP BY
#     u.user_id,
#     u.signup_date,
#     u.acquisition_channel,
#     u.platform,
#     u.country;


# In[24]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT
#     COUNT(*) AS total_users,
#     SUM(retained_day_60) AS retained_users,
#     ROUND(AVG(retained_day_60) * 100, 2) AS retention_pct
# FROM gold_user_retention;


# In[25]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT
#     days_since_signup,
#     COUNT(DISTINCT user_id) AS active_users
# FROM silver_sessions
# WHERE days_since_signup BETWEEN 55 AND 65
# GROUP BY days_since_signup
# ORDER BY days_since_signup;


# In[ ]:




