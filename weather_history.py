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
		with open('/home/essbase/credentials.txt', 'rb') as csvfile:
			reader = csv.reader(csvfile)
			for row in reader:
				credentials = row[0]
				conn = psycopg2.connect(credentials)
			try:
				self.conn = conn
			except:
        			print "I am unable to connect to 'edw'"

        def coordinates(self):
		sql = "select scode, concat(latitude, ',', longitude) as coordinates from staging.coordinates;"
		cur = self.conn.cursor()
		cur.execute(sql)

		self.rows = []
		self.rows = cur.fetchall()

		return self.rows
		self.conn.close()

class weather(db_connection):
	# API key to access Wunderground as biESSEX
	api_key = "a12345bcd67890ef"

	# date variable
	date = datetime.date.today() - datetime.timedelta(days=1)

	def query(self):
		# format the date as YYYYMMDD
		date_string = self.date.strftime('%Y%m%d')
			
		for c in self.coordinates():
			scode = c[0]
			coordinates = c[1]

			# build the URL
			url = ("http://api.wunderground.com/api/%s/geolookup/history_%s/q/%s.json" %
			(self.api_key, date_string, coordinates))

			# make the request and parse json
			data = requests.get(url).json()

			# build your row
			for history in data['history']['dailysummary']:
				date = str(history['date']['pretty'])
				month = history['date']['mon']
				day = history['date']['mday']
				year = history['date']['year']
				high = history['maxtempi']
				if high == "":
					high = None
				low = history['mintempi']
				if low == "":
					low = None
				rain = history['rain']

				sql = "INSERT INTO weather.history (scode, date, month, day, year, high, low, rain) values (%s, %s, %s, %s, %s, %s, %s, %s)"
				data = (scode, date, month, day, year, high, low, rain)

				cur = self.conn.cursor()
				cur.execute(sql, data)
				self.conn.commit()

			# delay the next call for six seconds, can only make up to 10 calls a minute
			time.sleep(6)
		# close the db connection
		self.conn.close()

### main ###
connection = weather()
connection.coordinates()
connection.query()
