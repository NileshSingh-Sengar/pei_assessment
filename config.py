# Databricks notebook source
CATALOG        = "pei_assessment" #can be edited
WORKSPACE_ROOT = "/Workspace/Shared/pei_assessment" #can be edited
BRONZE      = f"{CATALOG}.bronze"
SILVER      = f"{CATALOG}.silver"
GOLD        = f"{CATALOG}.gold"
VOLUME_BASE = f"/Volumes/{CATALOG}/bronze/landing"