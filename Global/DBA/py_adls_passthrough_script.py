# Databricks notebook source
configs = {"fs.azure.account.auth.type": "CustomAccessToken", "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")}
