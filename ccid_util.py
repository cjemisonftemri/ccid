# Databricks notebook source
def upcase_columns(df):
    return df.withColumnsRenamed(
        {x: x.upper() for x in df.columns}
    )
