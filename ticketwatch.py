import requests
import sqlite3
import sys
import logging
import time
from datetime import datetime, timedelta
import math
import pandas as pd
import re

# logging
# logging.basicConfig(format='%(asctime)s %(message)s', filename='ticketwatch.log', level=logging.INFO)
logger   = logging.getLogger('ticketwatch')
def crateLogHandler(filename, logLevel):
	handler = logging.FileHandler(filename)
	handler.setLevel(logLevel)
	handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
	return handler

log_warn = crateLogHandler("log/ticketwatch.log", logging.WARNING)
logger.addHandler( log_warn )
log_info = crateLogHandler("log/detail/ticketwatch-{:%y%m%d-%H%M%S}.log".format(datetime.now()), logging.INFO)
logger.addHandler( log_info )
logger.setLevel(logging.INFO)



useSandbox = False
# sandbox is about 33 days behind in data

if useSandbox:
	apiURL = 'https://api.stubhubsandbox.com'
	token = 'INSERT SANDBOX TOKEN HERE'
	verify = False
else:
	apiURL = 'https://api.stubhub.com'
	token = 'INSERT PRODUCTION TOKEN HERE'
	verify = True

# download events from Stubhub
headers = {
	'Authorization': "Bearer " + token, 
	'Accept': "application/json",
	'Accept-Encoding': "application/json",
}

# connect to sqlite database
conn = sqlite3.connect('data/ticketwatch.db')
cu = conn.cursor() 



def getListings(eventId):
	"""get ticket listings for a particular event"""

	payload = {
		'eventId': eventId, 
		'zoneStats': 'true',
		'priceType': 'currentPrice',
		'rows': 500, # get one if only getting zone stats, get 500 if also search listings for macthing listingPrice
	}
	
	ts = time.time()
	
	r = requests.get(
		apiURL + '/search/inventory/v2', 
		params=payload,
		headers=headers, 
		verify=verify
	)
	

	if   r.status_code==400:
		jdoc = r.json()
		logger.error("Get listings for event %d failed: there were errors in the request. Code %s: %s", eventId, jdoc['code'], jdoc['description'])
		ret = -1
	elif r.status_code==401:
		logger.error("Get listings for event %d failed: The server didn't recognize your authorization token.", eventId)
		ret = -1
	elif r.status_code==500:
		logger.error("Get listings for event %d failed: The server generated an unspecified error.", eventId)
		ret = -1
	elif r.status_code==429:
		logger.error("Get listings for event %d failed: Too many requests.", eventId)
		# quitWhile = True;
		time.sleep(60)
		ret = -1
	elif r.status_code==200:
		
		# download listings 
		jdoc = r.json()
		if jdoc['totalListings']>0:
			if 'zone_stats' in jdoc: # event has zones
				for zone in jdoc['zone_stats']:
					# zone_stats only contains lowest _current_ price.  see if the matching lowest _listing_ price appears in the downloaded listings

					# # code using while loop
					# li=0; notfound = True; lowestListingPrice = None 
					# while notfound and li<len(jdoc['listing']):
						# listing = jdoc['listing'][li]
						# if listing['zoneId']==zone['zoneId'] and listing['currentPrice']['amount']==zone['minTicketPrice']:  # first listing that it hits with the right zoneId should be cheapest, but just to be sure.
							# lowestListingPrice = jdoc['listing'][li]['listingPrice']['amount']
							# notfound = False
						# else:
							# li+=1

					# code using a for loop
					for listing in jdoc['listing']:
						if 'zoneId' in listing:
							if listing['zoneId']==zone['zoneId'] and listing['currentPrice']['amount']==zone['minTicketPrice']:
								lowestListingPrice = listing['listingPrice']['amount']
								break
					else: # if no listing is found for that zone with the right price
						lowestListingPrice = None
					
					cu.execute("INSERT INTO listings (timestamp, eventid, zonename, lowestCurrentPrice, lowestListingPrice, totalTickets) VALUES (?, ?, ?, ?, ?, ?)", (ts, eventId, zone['zoneName'], zone['minTicketPrice'], lowestListingPrice, zone['totalTickets']))
			else: # general admission only
				cu.execute("INSERT INTO listings (timestamp, eventid, zonename, lowestCurrentPrice, lowestListingPrice, totalTickets) VALUES (?, ?, ?, ?, ?, ?)", (ts, eventId, "GENADM", jdoc['listing'][0]['currentPrice']['amount'], jdoc['listing'][0]['listingPrice']['amount'], jdoc['totalTickets']))
			logger.info("Got listings for event %d.", eventId)
			ret = 0
		else:
			logger.info("No listings for event %d.", eventId)
			ret = -1
	else: # unknown HTTP status code
		logger.error("Get listings for event %d failed: unknown status code %d", eventId, r.status_code)
		ret = -1
		
	return ret
	

	
	

def getAllListings():
	"""for each event in events table, get ticket listings and update status if necessary"""


	# get active events
	nsuccess = 0; nfailed = 0
	cu.execute("SELECT id, (eventTS-strftime('%s','now'))/86400.0 AS daysToEvent FROM events WHERE status=2")
	results = cu.fetchall()
	
	
	freqs = [(r[0], (min(max(8/24,r[1]),364.9)/365.0)**(-1/3)) for r in results]
	queue = []
	for f in freqs:
		x = 1/(2*f[1])
		while x<1:
			queue.append((f[0],x))
			x+=1/f[1]

	queue.sort(key=lambda q: q[1])
	
	for event in queue:

		eventId = event[0]
		
		try:
			status = getListings(eventId)
			time.sleep(6)
			conn.commit()  
			
		except Exception as err:
			logger.error("Runtime error at event %d: %s", eventId, err)
			time.sleep(6)
		

		
# ## NEW CODE ## #

def getEventInfo(eventId):
	""" getEventInfo(eventId)
	Downloads event information through Stubhub API and logs any errors. 
	Returns a HTTP response object.  
		r.status_code: HTTP status code is 200 if no problems.
		r.json():      Data in JSON form.
	"""
	r = requests.get(
		apiURL + '/catalog/events/v2/' + str(eventId), 
		headers=headers, 
		verify=verify
	)
	
	if   r.status_code==400:
		logger.error("Event %d lookup failed: eventID isn't a positive integer.", eventId)
	elif r.status_code==401:
		logger.error("Event %d lookup failed: The server didn't recognize your authorization token.", eventId)
	elif r.status_code==404:
		logger.error("Event %d lookup failed: eventId doesn't identify an event.", eventId)
	elif r.status_code==500:
		logger.error("Event %d lookup failed: The server generated an unspecified error.", eventId)
	elif r.status_code==429:
		logger.error("Event %d lookup failed: Too many requests.", eventId)
		time.sleep(60)
	elif r.status_code==200: # no problems
		pass
	else: # unknown HTTP status code
		logger.error("Event %d lookup failed: unknown status code %d", eventId, r.status_code)
	
	return r
	
	
def updateEvent(eventId, log=True):
	""" updateEvent(eventId)
	Downloads event info and updates event in events table if there's a change. 
	Returns a tuple (code, oldValues, newValues):
		code = -1 for error getting event info. oldValues and newValues will be empty tuples. 
		code =  0 for no change found. oldValues and newValues should be the same. 
		code =  1 for an update made to the event. returns oldValues and newValues. 
	""" 
	
	eventId = int(eventId)  # numpy.int64 type was causing problems.
	r = getEventInfo(eventId)
	if r.status_code!=200:  # HTTP request had a problem
		return (-1, (), ())
	else: # no HTTP problems
	
		jdoc = r.json()
		eventDT = datetime.strptime(jdoc['eventDateLocal'].replace('Z','+0000').replace(':', ''), "%Y-%m-%dT%H%M%S%z")  # in single event API, time zone has an extra colon that the strptime(~,'%z') won't parse
		
		eventColumns = ("eventTS", "tz", "venueid", "status", "currency")
		old = cu.execute("SELECT {} FROM events WHERE id=?".format(",".join(eventColumns)), (eventId,)).fetchone()
		new = (int(eventDT.timestamp()), int(eventDT.utcoffset().total_seconds()), 
		       jdoc['venue']['id'], jdoc['status']['statusId'], jdoc['currencyCode'])
		
		# if any of these have changed, update! 
		if old!=new:
			updateQuery = ", ".join([c + "=?" for c in eventColumns])  # outputs the string "id=?, eventTS=? ..."
			updateQuery = "UPDATE events SET {} WHERE id=?".format(updateQuery)
			cu.execute(updateQuery, new + (eventId,) )
			if log: 
				statusCol = eventColumns.index("status")
				# no need to log a warning if only status has changed to 5 - event has completed
				if (all([old[i]==new[i] for i in range(len(eventColumns)) if i!=statusCol]) and new[statusCol]==5): 
					logger.info("Event %d completed.", eventId)
				else:  # more significant update
					logger.warning("Event %d updated.\n\told: %s\n\tnew: %s", eventId, old, new)
			changed = 1
		else:
			changed = 0
			if log: logger.info("Event %d unchanged.", eventId)
	

	return (changed, old, new)


def getEventsGen(eventType="BayAreaConcerts"): 
	""" getEventsGen()
	Generates a page of events found. Yields a list of event dicts, or [] if 
	something went wrong. 
	"""
	start = 0
	maxrows = 500
	numFound = 1
	
	oneMinuteAgoUTC = datetime.utcnow() + timedelta(minutes=-1)
	
	while (start<numFound):

		# default is San Francisco Bay Area concerts
		payload = {
			'geoId': 81, # 81 = San Francisco Bay Area, 4 = California
			'rows': maxrows,  # max is 500, default is 10
			'start': start,
			'sort': "eventDateLocal asc",
			'categoryId': 1,  # 1 = Concert
	#		'status': 'active',
			'parking': 'false',
			'createdDate': "2015-01-01T00:00 TO {}".format( oneMinuteAgoUTC.strftime("%Y-%m-%dT%H:%M") )   # so that multiple calls (separated by 6 seconds) refer to the same set 
		}

		if eventType=="BayAreaConcerts":  # San Francisco Bay Area concert events 
			pass
		elif eventType=="NFL":  # NFL games
			payload = {
				'rows': maxrows,  # max is 500, default is 10
				'start': start,
				'sort': "eventDateLocal asc",
				'groupingId': 121,  # 121 = 2016 NFL tickets, 80916 = 2016 NFL Regular Season
		#		'status': 'active',
				'parking': 'false',
				'createdDate': "2015-01-01T00:00 TO {}".format( oneMinuteAgoUTC.strftime("%Y-%m-%dT%H:%M") )   # so that multiple calls (separated by 6 seconds) refer to the same set 
			}
		else: 
			logger.error("Unknown event type: %s. Defaulting to Bay Area Concerts.", eventType)
		
			
		# send get request
		r = requests.get(
			apiURL + '/search/catalog/events/v3', 
			params=payload, 
			headers=headers, 
		#	verify='stubhubsandbox.pem',
			verify=verify
		)

		
		ret = []
		numFound = 0
		if   r.status_code==400:
			logger.error("Get events failed: Bad request.")
		elif r.status_code==401:
			logger.error("Get events failed: The server didn't recognize your authorization token.")
		elif r.status_code==403:
			logger.error("Get events failed: Forbidden.")
		elif r.status_code==500:
			logger.error("Get events failed: The server generated an unspecified error.")
		elif r.status_code==429:
			logger.error("Get events failed: Too many requests.")
			time.sleep(60)
		elif r.status_code==200:  # successful request
			# success
			jdoc = r.json()
			ret = jdoc['events']
			numFound = jdoc['numFound'] 
			
		else: # unknown HTTP status code
			logger.error("Get events failed: unknown status code %d", r.status_code)
		
		yield ret
		start += maxrows
		time.sleep(6)

	
def getAndUpdateEvents():
	
	eventsFound  = sum(list(getEventsGen("BayAreaConcerts")), []) + sum(list(getEventsGen("NFL")), [])
	
	# put events table into DataFrame
	eventColumns = ("id", "name", "eventTS", "tz", "eventDateLocal", "venueid", "status", "createdTS", "imageUrl")
	eventsInDb   = pd.DataFrame(cu.execute("SELECT {} FROM events".format(", ".join(eventColumns))).fetchall(), 
							   columns=eventColumns)
	eventsInDb.set_index("id", drop=True, inplace=True)
	
	# 2. Active. The event has not yet taken place.
    # 3. Contingent. The event is contingent on some other event. For example, a sports playoff match may depend on the outcome of an earlier match.
    # 4. Canceled. The event has been canceled.
    # 5. Completed. The event's date is in the past.
    # 6. Postponed. The event has been postponed.
    # 7. Scheduled. The event has been scheduled, but it's too far in the future to accept listings.
	statusCodes = {'Active': 2, 'Contingent': 3, 'Canceled': 4, 'Completed': 5, 'Postponed': 6, 'Scheduled': 7}
	
	
	eventsInsertedUpdated = []
	# ## insert or update events 
	for event in eventsFound: 
		try:
			eventDateLocalDT = datetime.strptime(event['eventDateLocal'], "%Y-%m-%dT%H:%M:%S%z")
			createdDateDT = datetime.strptime(event['createdDate'], "%Y-%m-%dT%H:%M:%S%z")
			eventName = re.sub("(Tickets)? \([^\)]+\)", "", event['name']).strip()  # remove things like " Tickets (18+ Event)" or "(Rescheduled from ...)"
			values = (eventName, int(eventDateLocalDT.timestamp()), int(eventDateLocalDT.utcoffset().total_seconds()), 
					  eventDateLocalDT.strftime("%Y-%m-%d %H:%M:%S"), event['venue']['id'], statusCodes[event['status']], 
					  int(createdDateDT.timestamp()), event['imageUrl'])
		
			if event['id'] in eventsInDb.index:  # if event exists in database
				# if anything has changed, update
				if values!=tuple(eventsInDb.loc[event['id']]):
					updateQuery = ", ".join([c + "=?" for c in eventColumns[1:]])  # outputs the string "name=?, eventTS=? ..."
					updateQuery = "UPDATE events SET {} WHERE id=?".format(updateQuery)
					cu.execute(updateQuery, values + (event['id'],) )
					logger.warning("Updated event %d:\n\told: %s\n\tnew: %s", event['id'], tuple(eventsInDb.loc[event['id']]), values)
					
				# # update venue - RUN ONCE, REMOVE
				# if 'address2' in event['venue']:
					# address2 = event['venue']['address2']
				# else:
					# address2 = ''
				# cu.execute("UPDATE venues SET name=?, address1=?, address2=?, city=?, country=?, state=?, postalCode=?, latitude=?, longitude=? WHERE id=?", 
				           # (event['venue']['name'], event['venue']['address1'], 
							# address2, event['venue']['city'], event['venue']['country'], event['venue']['state'], 
							# event['venue']['postalCode'], event['venue']['latitude'], event['venue']['longitude'], event['venue']['id']))
				# if cu.rowcount>0:
					# logger.info("Venue %d updated.", event['venue']['id'])
				
			else:  # if event does not exist in database, insert 
				
				insertQuery = "INSERT INTO events ({}) VALUES ({})".format(
								  ",".join(eventColumns),              # outputs string "id, name, eventTS ..."
								  ",".join(len(eventColumns)*('?',)),  # outputs string "?, ?, ... ?"
							  )
				cu.execute( insertQuery, (event['id'],) + values )
				logger.info("Inserted event %d: %s", event['id'], eventName)

				# add new found categories, and add event/category associations
				for category in event['categories']:
					if category['id']!=0:  # category 0 is all events; has no name
						cu.execute("INSERT OR IGNORE INTO categories VALUES (?, ?)", (category['id'], category['name']))
						if cu.rowcount>0:
							logger.info("Inserted category %d: %s", category['id'], category['name'])
						cu.execute("INSERT INTO eventcategories VALUES (?,?,0)", (event['id'], category['id']))
				# # same for categoriesCollection
				for category in event['categoriesCollection']:
					if category['id']!=0:  
						cu.execute("INSERT OR IGNORE INTO categories VALUES (?, ?)", (category['id'], category['name']))
						if cu.rowcount>0:
							logger.info("Inserted categoryCollection %d: %s", category['id'], category['name'])
						cu.execute("INSERT INTO eventcategories VALUES (?,?,1)", (event['id'], category['id']))

				# add new found groupings, and add event/grouping associations
				for grouping in event['groupings']:
					if grouping['id']!=0:  # grouping 0 is all events; has no name
						cu.execute("INSERT OR IGNORE INTO groupings VALUES (?, ?)", (grouping['id'], grouping['name']))
						if cu.rowcount>0: 
							logger.info("Inserted grouping %d: %s", grouping['id'], grouping['name'])
						cu.execute("INSERT INTO eventgroupings VALUES (?,?,0)", (event['id'], grouping['id']))
				# # same for groupingsCollection
				if 'groupingsCollection' in event:
					for grouping in event['groupingsCollection']:
						if grouping['id']!=0:  
							cu.execute("INSERT OR IGNORE INTO groupings VALUES (?, ?)", (grouping['id'], grouping['name']))
							if cu.rowcount>0: 
								logger.info("Inserted groupingCollection %d: %s", grouping['id'], grouping['name'])
							cu.execute("INSERT INTO eventgroupings VALUES (?,?,1)", (event['id'], grouping['id']))

				# add performers to performers db, and add event/performer associations
				if 'performers' in event:
					for performer in event['performers']:
						if performer['id']!=0:
							cu.execute("INSERT OR IGNORE INTO performers VALUES (?, ?)", (performer['id'], performer['name']))
							if cu.rowcount>0: 
								logger.info("Inserted performer %d: %s", performer['id'], performer['name'])
							if 'role' in performer:
								role = performer['role']
							else:
								role = ''
							cu.execute("INSERT INTO eventPerformers VALUES (?,?,0,?)", (event['id'], performer['id'], role)) 
				if 'performersCollection' in event:
					for performer in event['performersCollection']:
						if performer['id']!=0:
							cu.execute("INSERT OR IGNORE INTO performers VALUES (?, ?)", (performer['id'], performer['name']))
							if cu.rowcount>0: 
								logger.info("Inserted performerCollection %d: %s", performer['id'], performer['name'])
							if 'role' in performer:
								role = performer['role']
							else:
								role = ''
							cu.execute("INSERT INTO eventPerformers VALUES (?,?,1,?)", (event['id'], performer['id'], role)) 

				# add venue 
				if 'address2' in event['venue']:
					address2 = event['venue']['address2']
				else:
					address2 = ''

				cu.execute("INSERT OR IGNORE INTO venues VALUES (?,?,?,?,?,?,?,?,?,?)", (event['venue']['id'], event['venue']['name'], event['venue']['address1'], 
																					address2, event['venue']['city'], event['venue']['country'], event['venue']['state'], 
																					event['venue']['postalCode'], event['venue']['latitude'], event['venue']['longitude']))
				if cu.rowcount>0: 
					logger.info("Inserted venue %d: %s", event['venue']['id'], event['venue']['name'])

				
				# also get currency
				r = getEventInfo( event['id'] )
				if r.status_code==200:
					jdoc = r.json()
					cu.execute( "UPDATE events SET currency=? WHERE id=?", (jdoc['currencyCode'], event['id']) )
					logger.info("Got currency for event %d.", event['id'])
				time.sleep(6)
		
			# if everything worked, insert event into list of inserted/updated events
			eventsInsertedUpdated.append(event['id'])
			conn.commit()
				
		except Exception as err:
			if 'id' in event:
				logger.error("Failed to insert event %d: %s", event['id'], err)
			else:
				logger.error("Failed to insert event with unknown id: %s", err)


				
	# ## update database events that are not in the downloaded set and are active
	events = eventsInDb[~eventsInDb.index.isin(eventsInsertedUpdated) & list(eventsInDb['status']==2)]

	for eventRow in events.iterrows():
		
		eventId = eventRow[0]
		try: 
			updateEvent(eventId)
			conn.commit()
		except Exception as err:
			if 'id' in event:
				logger.error("Failed to update event %d: %s", eventId, err)
			else:
				logger.error("Failed to update event with unknown id: %s", err)
		time.sleep(6)


	
		
def main(argv):
	usage = "Usage: python3 ticketwatch.py [getlistings | getevents [date] | updateevent eventid | getandupdateevents ]"
	if not argv:
		print(usage)
	elif argv[0].upper()=="GETLISTINGS":
		getAllListings()
	elif argv[0].upper()=="GETEVENTS":
		getEventsByDay(argv[1:])
	elif argv[0].upper()=="GETANDUPDATEEVENTS":
		getAndUpdateEvents()
	elif argv[0].upper()=="RUN":
		logger.removeHandler( log_info )
		
		while True:
			logHandler = crateLogHandler("log/detail/ticketwatch-{:%y%m%d-%H%M%S}.log".format(datetime.now()), logging.INFO)
			logger.addHandler( logHandler )
			
			logger.log(logging.ERROR, "Running getAndUpdateEvents.")
			getAndUpdateEvents()
			
			logger.log(logging.ERROR, "Running getAllListings.")
			getAllListings()
			
			logger.removeHandler( logHandler )
			
	elif argv[0].upper()=="UPDATEEVENT":
		if argv[1]:
			updateEventStatus(int(argv[1]))
		else:
			print(usage)

	else:
		print(usage)

		
if __name__ == "__main__":
    main(sys.argv[1:])
else:
	main([])
	
conn.commit()
cu.close()
conn.close()

