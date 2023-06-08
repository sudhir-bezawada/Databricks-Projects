# Databricks notebook source
import datetime
import socket

print ("\n============================================\n    This is from the included notebook\n============================================\n")

today = datetime.date.today()
print("Today's date is:         ", today)
print("Today's date and time is:", datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
print("Current year:            ", datetime.date.today().strftime("%Y"))
print("Month of year:           ", datetime.date.today().strftime("%B"))
print("Week number of the year: ", datetime.date.today().strftime("%W"))
print("Weekday of the week:     ", datetime.date.today().strftime("%w"))
print("Day of year:             ", datetime.date.today().strftime("%j"))
print("Day of the month :       ", datetime.date.today().strftime("%d"))
print("Day of week:             ", datetime.date.today().strftime("%A"))
print("**** Hostname ****:      ", socket.gethostname())
