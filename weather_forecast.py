#!/usr/bin/python

import psycopg2
import requests
import datetime
import urllib2
import json
import time
import csv

class db_connection(object):
	def __init__(self):
		try:
			self.conn = psycopg2.connect("dbname='edw' user='admin' host='localhost' password='password'")	
		except:
			print "I am unable to connect to 'edw'"

        def coordinates(self):
		truncate = "truncate table weather.forecast;"
		cur = self.conn.cursor()
		cur.execute(truncate)

                sql = "select scode, concat(latitude, ',', longitude) as coordinates from staging.coordinates;"
                cur = self.conn.cursor()
                cur.execute(sql)

                self.rows = []
                self.rows = cur.fetchall()

                return self.rows
                self.conn.close()

class weather(db_connection):
		
	# API key to access Wunderground as biESSEX
	api_key = "a1234bcd56789ef"
	def query(self):
		for c in self.coordinates():
			scode = c[0]
			coordinates = c[1]

			# build the URL
			url = ("http://api.wunderground.com/api/%s/geolookup/forecast/q/%s.json" %
				(self.api_key, coordinates))

			# make the request and parse json
			data = requests.get(url).json()
	
			# build your row
			for forecast in data['forecast']['simpleforecast']['forecastday']:
				row = []
				row.append(scode)
				row.append(str(forecast['date']['pretty']))
				row.append(forecast['date']['month'])
				row.append(forecast['date']['day'])
				row.append(forecast['date']['year'])
				row.append(str(forecast['date']['monthname_short']))
				row.append(str(forecast['date']['weekday']))
				row.append(str(forecast['date']['weekday_short']))
				row.append(forecast['high']['fahrenheit'])
				row.append(forecast['low']['fahrenheit'])
				row.append(str(forecast['conditions']))
			
				# writer.writerow(row)
				sql = "INSERT INTO weather.forecast (scode, date, month, day, year, abbr_month_name, long_weekday_name, abbr_weekday_name, high, low, conditions) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
			
				cur = self.conn.cursor()
				cur.execute(sql, row)
				self.conn.commit()
				
			# delay the next call for six seconds
			time.sleep(6)

		self.conn.close()

### main ###
connection = weather()
connection.query()
