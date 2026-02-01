# Databricks notebook source
parameters = [
    {
        "table" : "apotify_cata.silver.factstream",
        "alias" : "factstream",
        "col" : "factstream.stream_id, factstream.listen_duration"
    },
    {
        "table" : "apotify_cata.silver.dimuser",
        "alias" : "dimuser",
        "col" : "dimuser.user_id, dimuser.user_name",
        "condition" : "factstream.user_id = dimuser.user_id"
    },
    {
        "table" : "apotify_cata.silver.dimtrack",
        "alias" : "dimtrack",
        "col" : "dimtrack.track_id, dimtrack.track_name",
        "condition" : "factstream.track_id = dimtrack.track_id"
    },
]

# COMMAND ----------

pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

query = """
    select
        {% for i in parameters %}
            {{ i['col'] }}
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    from
        {% for i in parameters %}
            {% if loop.first %}
                {{ i['table'] }} as {{ i['alias'] }}
            {% else %}
            left join 
                {{ i['table'] }} as {{ i['alias'] }}
            on  
                {{ i['condition'] }}
            {% endif %}
        {% endfor %}
"""


# COMMAND ----------

jinja_sql_query = Template(query)
code = jinja_sql_query.render(parameters=parameters)
print(code)

# COMMAND ----------

display(spark.sql(code))

# COMMAND ----------

