# Databricks notebook source
LLM Code 



# 0️⃣ Explicitly initialize Spark
from pyspark.sql import SparkSession

# 1️⃣ Load the EmailMessage table
emails = spark.table("source_systems.salesforce.emailmessage").select(
    "Id", "Subject", "TextBody", "MessageDate", "FromAddress", "ToAddress"
)

# 2️⃣ Clean the email text using Spark functions only
from pyspark.sql import functions as F

emails = emails.withColumn(
    "CleanedEmail",
    F.regexp_replace(F.regexp_replace(F.col("TextBody"), "\n", " "), "\r", " ")
)

# 3️⃣ Connect to Databricks Model Serving
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
import json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

w = WorkspaceClient()

# 4️⃣ Define a function that can be used safely in Spark UDF
def analyze_email_safe(email_text: str) -> str:
    """Return JSON string of analysis or raw output on error."""
    if not email_text:
        return json.dumps({"raw_output": None})

    prompt_text = f"""
You are analyzing a Salesforce support email. 

Extract the following fields in JSON format:
{{
  "sentiment": "positive | neutral | negative",
  "issue": "<main issue>",
  "reference_numbers": ["..."],
  "urgent": true | false,
  "summary": "<2-3 sentence summary>"
}}

Email:
{email_text}
"""
    try:
        resp = w.serving_endpoints.query(
            name="databricks-meta-llama-3-3-70b-instruct",
            messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt_text)]
        )

        if resp.predictions and len(resp.predictions) > 0:
            candidates = resp.predictions[0].get("candidates")
            if candidates and len(candidates) > 0:
                text = candidates[0].get("text")
                if text:
                    return text
        return json.dumps({"raw_output": None})

    except Exception as e:
        return json.dumps({"raw_output": f"Error: {str(e)}"})

# 5️⃣ Register UDF safely (return StringType to avoid pandas conversions)
from pyspark.sql.functions import udf
analyze_udf = udf(analyze_email_safe, StringType())

emails = emails.withColumn("AnalysisJSON", analyze_udf(F.col("CleanedEmail")))

# 6️⃣ Extract fields from JSON safely using Spark SQL functions
from pyspark.sql.functions import from_json

schema = StructType([
    StructField("sentiment", StringType(), True),
    StructField("issue", StringType(), True),
    StructField("reference_numbers", StringType(), True),
    StructField("urgent", BooleanType(), True),
    StructField("summary", StringType(), True),
    StructField("raw_output", StringType(), True)
])

emails_parsed = emails.withColumn("Analysis", from_json(F.col("AnalysisJSON"), schema))

# 7️⃣ Flatten the JSON fields
final_df = emails_parsed.select(
    F.col("Id").alias("EmailId"),
    F.col("Subject").alias("EmailSubject"),
    F.col("Analysis.sentiment").alias("Sentiment"),
    F.col("Analysis.issue").alias("Issue"),
    F.col("Analysis.reference_numbers").alias("ReferenceNumbers"),
    F.col("Analysis.urgent").alias("Urgent"),
    F.col("Analysis.summary").alias("Summary"),
    F.col("Analysis.raw_output").alias("RawOutput")
)

# 8️⃣ Save to Delta table
final_df.write.mode("overwrite").saveAsTable(
    "analysts.idm_and_settlement.Salesforce_Email_Insights"
)

print("Saved analysis to analysts.idm_and_settlement.Salesforce_Email_Insights")
