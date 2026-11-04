#!/usr/bin/env python
# coding: utf-8

# ## run_gold_layer
# 
# null

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT 
#     percentile_approx(days_since_signup, 0.25) AS p25,
#     percentile_approx(days_since_signup, 0.5) AS median,
#     percentile_approx(days_since_signup, 0.75) AS p75,
#     MIN(days_since_signup) AS min_days,
#     MAX(days_since_signup) AS max_days
# FROM silver_goals


# In[2]:


retention_day60 = spark.sql("""
    SELECT DISTINCT user_id
    FROM silver_sessions
    WHERE days_since_signup BETWEEN 53 AND 67
""")
retention_day60.createOrReplaceTempView("retained_day60_users")

print("Users retained at day 60:", retention_day60.count())


# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW early_goal_flag AS
# SELECT 
#     u.user_id,
#     CASE 
#         WHEN g.days_since_signup <= 14 THEN 1 
#         ELSE 0 
#     END AS early_goal_creator
# FROM silver_users u
# LEFT JOIN silver_goals g ON u.user_id = g.user_id


# In[4]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW pre_landmark_sessions AS
# SELECT 
#     user_id,
#     COUNT(*) AS sessions_before_day14
# FROM silver_sessions
# WHERE days_since_signup < 14
# GROUP BY user_id


# In[5]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TABLE gold_user_retention AS
# SELECT
#     u.user_id,
#     u.signup_date,
#     u.acquisition_channel,
#     u.platform,
#     u.country,
#     CASE WHEN r60.user_id IS NOT NULL THEN 1 ELSE 0 END AS retained_day_60,
#     COALESCE(eg.early_goal_creator, 0) AS early_goal_creator,
#     CASE WHEN g.user_id IS NOT NULL THEN 1 ELSE 0 END AS created_real_goal,
#     g.days_since_signup AS days_to_first_goal,
#     COALESCE(pl.sessions_before_day14, 0) AS sessions_before_day14
# FROM silver_users u
# LEFT JOIN retained_day60_users r60 ON u.user_id = r60.user_id
# LEFT JOIN early_goal_flag eg ON u.user_id = eg.user_id
# LEFT JOIN silver_goals g ON u.user_id = g.user_id
# LEFT JOIN pre_landmark_sessions pl ON u.user_id = pl.user_id


# In[6]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT COUNT(*) AS total_rows, COUNT(DISTINCT user_id) AS distinct_users
# FROM gold_user_retention


# In[7]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT 
#     early_goal_creator,
#     COUNT(*) AS users,
#     ROUND(100.0 * SUM(retained_day_60) / COUNT(*), 1) AS pct_retained_day60,
#     ROUND(AVG(sessions_before_day14), 2) AS avg_sessions_before_landmark
# FROM gold_user_retention
# GROUP BY early_goal_creator


# In[8]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW retained_day1_users AS
# SELECT DISTINCT user_id FROM silver_sessions WHERE days_since_signup BETWEEN 0 AND 2


# In[9]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW retained_day7_users AS
# SELECT DISTINCT user_id FROM silver_sessions WHERE days_since_signup BETWEEN 4 AND 10


# In[10]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW retained_day30_users AS
# SELECT DISTINCT user_id FROM silver_sessions WHERE days_since_signup BETWEEN 25 AND 35


# In[11]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW retained_day60_users AS
# SELECT DISTINCT user_id FROM silver_sessions WHERE days_since_signup BETWEEN 53 AND 67


# In[12]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW early_goal_flags AS
# SELECT 
#     u.user_id,
#     CASE WHEN g.days_since_signup <= 14 THEN 1 ELSE 0 END AS early_goal_creator_day14,
#     CASE WHEN g.days_since_signup <= 23 THEN 1 ELSE 0 END AS early_goal_creator_day23
# FROM silver_users u
# LEFT JOIN silver_goals g ON u.user_id = g.user_id


# In[13]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TEMP VIEW pre_landmark_sessions AS
# SELECT 
#     user_id,
#     SUM(CASE WHEN days_since_signup < 14 THEN 1 ELSE 0 END) AS sessions_before_day14,
#     SUM(CASE WHEN days_since_signup < 23 THEN 1 ELSE 0 END) AS sessions_before_day23
# FROM silver_sessions
# GROUP BY user_id


# In[14]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TABLE gold_user_retention AS
# SELECT
#     u.user_id,
#     u.signup_date,
#     u.acquisition_channel,
#     u.platform,
#     u.country,
#     CASE WHEN r1.user_id IS NOT NULL THEN 1 ELSE 0 END AS retained_day_1,
#     CASE WHEN r7.user_id IS NOT NULL THEN 1 ELSE 0 END AS retained_day_7,
#     CASE WHEN r30.user_id IS NOT NULL THEN 1 ELSE 0 END AS retained_day_30,
#     CASE WHEN r60.user_id IS NOT NULL THEN 1 ELSE 0 END AS retained_day_60,
#     COALESCE(eg.early_goal_creator_day14, 0) AS early_goal_creator_day14,
#     COALESCE(eg.early_goal_creator_day23, 0) AS early_goal_creator_day23,
#     CASE WHEN g.user_id IS NOT NULL THEN 1 ELSE 0 END AS created_real_goal,
#     g.days_since_signup AS days_to_first_goal,
#     COALESCE(pl.sessions_before_day14, 0) AS sessions_before_day14,
#     COALESCE(pl.sessions_before_day23, 0) AS sessions_before_day23
# FROM silver_users u
# LEFT JOIN retained_day1_users  r1  ON u.user_id = r1.user_id
# LEFT JOIN retained_day7_users  r7  ON u.user_id = r7.user_id
# LEFT JOIN retained_day30_users r30 ON u.user_id = r30.user_id
# LEFT JOIN retained_day60_users r60 ON u.user_id = r60.user_id
# LEFT JOIN early_goal_flags eg ON u.user_id = eg.user_id
# LEFT JOIN silver_goals g ON u.user_id = g.user_id
# LEFT JOIN pre_landmark_sessions pl ON u.user_id = pl.user_id


# In[15]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT COUNT(*) AS total_rows, COUNT(DISTINCT user_id) AS distinct_users
# FROM gold_user_retention


# In[16]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT 
#     early_goal_creator_day23,
#     COUNT(*) AS users,
#     ROUND(100.0 * SUM(retained_day_1)/COUNT(*), 1) AS pct_day1,
#     ROUND(100.0 * SUM(retained_day_7)/COUNT(*), 1) AS pct_day7,
#     ROUND(100.0 * SUM(retained_day_30)/COUNT(*), 1) AS pct_day30,
#     ROUND(100.0 * SUM(retained_day_60)/COUNT(*), 1) AS pct_day60,
#     ROUND(AVG(sessions_before_day23), 2) AS avg_sessions_before_landmark
# FROM gold_user_retention
# GROUP BY early_goal_creator_day23


# In[17]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TABLE dim_date AS
# SELECT
#     explode(sequence(to_date('2026-01-01'), to_date('2026-05-31'), interval 1 day)) AS date
# FROM (SELECT 1)


# In[18]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE OR REPLACE TABLE dim_date AS
# SELECT
#     date,
#     YEAR(date) AS year,
#     MONTH(date) AS month,
#     DAY(date) AS day,
#     DATE_FORMAT(date, 'EEEE') AS day_name,
#     CASE WHEN DAYOFWEEK(date) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
#     WEEKOFYEAR(date) AS week_of_year
# FROM dim_date


# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# DESCRIBE gold_user_retention


# In[ ]:




