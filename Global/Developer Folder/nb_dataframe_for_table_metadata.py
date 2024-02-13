# Databricks notebook source
df_add = df_dist.withColumn("SnapshotTimestamp",lit(datetime.now()))\
                           .withColumn("ETLInsertBatchID",lit('-1'))\
                           .withColumn("RecordInsertDateTime",lit(datetime.now()))\
                           .withColumn("RecordInsertUserName",lit("Azure_Dbrics_User"))\
                           .withColumn("RecordUpdateDateTime",lit(datetime.now()))\
                           .withColumn("ETLUpdateBatchID",lit('-1'))\
                           .withColumn("RecordUpdateUserName",lit("Azure_Dbrics_User"))
