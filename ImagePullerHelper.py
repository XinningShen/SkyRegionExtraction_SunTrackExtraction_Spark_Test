import os			# To traverse files in the OS
import re			# For regex matching
import zipfile		# To zip the files
import MySQLdb		# To connect and query to the MYSQL
import rethinkdb	# To connect and query to the RETHINKDB
import tempfile		# Will probably replace my dump folder with tempfile eventually
import calendar		# For timegm (makes uncorrected epoch time)
import time			# To format times
import urllib		# To "download" files
import shutil		# For removing files
import cStringIO	# To "save" "downloaded" files on "disk" (it's really in-memory)
import base64

def decode(key, enc):
  dec = []
  enc = base64.urlsafe_b64decode(enc)
  for i in range(len(enc)):
      key_c = key[i % len(key)]
      dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
      dec.append(dec_c)
  return "".join(dec)


def validate(dev_id, d1, t1, d2, t2):
	values = [str(dev_id), str(d1), str(t1), str(d2), str(t2)]

	# Note: 2 ways to do the same thing
	# (Although mapping won't work for the second case)
	map(str.strip, values)
	values = [item.replace(' ', '') for item in values]
	values = [item.lower() for item in values]
	values[0] = values[0].upper()

	for counter, item in enumerate(values):
		if counter == 0:
			if not re.match('^04E67[\S]{7}$', item):
				values = ['ERR', 'Bad DeviceID']
		elif (counter == 1 or counter == 3):			# Possible dates
			if re.match('^\d+-\d+-\d+$', item):			# Check for valid date pattern
				fields = item.split('-')
				if len(fields[0]) != 4:					# Check valid year
					if values[0] != 'ERR':
						values = ['ERR']
					values.append('Bad year_'+str((counter+1)/2))
				if 1 <= int(fields[1]) <= 12:			# Check valid month
					continue
				else:
					if values[0] != 'ERR':
						values = ['ERR']
					values.append('Bad month_'+str((counter+1)/2))
				if 1 <= int(fields[2]) <= 31:			# Check valid day
					continue
				else:
					if values[0] != 'ERR':
						values = ['ERR']
					values.append('Bad day_'+str(counter))
				if values[0] != 'ERR':
					values[counter] = '-'.join(fields)
			else:										# Invalid date format
				if values[0] != 'ERR':
					values = ['ERR']
				values.append('Bad format for date_'+str((counter+1)/2))
		elif(counter == 2 or counter == 4):				# Possible times
			if re.match('^\d+:\d+am|\d+:\d+pm$', item):	# Check for valid time pattern
				fields = item.split(':')
				if 0 <= int(fields[0]) <= 12:			# Check valid hour
					if (fields[1][-2:] == 'am' and		# Convert to 24 hour format
						int(fields[0]) == 12):
							fields[0] = '00'
					elif (fields[1][-2:] == 'pm' and
						int(fields[0]) != 12):
							fields[0] = str(int(fields[0])+12)
				else:
					if values[0] != 'ERR':
						values = ['ERR']
					values.append('Bad hour_'+str(counter/2))
				if 0 <= int(fields[1][0:-2]) <= 59:		# Check valid minute
					fields[1] = fields[1][0:-2]
				else:
					if values[0] != 'ERR':
						values = ['ERR']
					values.append('Bad minute_'+str(counter/2))
				if values[0] != 'ERR':
					values[counter] = ':'.join(fields)
			else:										# Invalid time format
				if values[0] != 'ERR':
						values = ['ERR']
				values.append('Bad format for time_'+str(counter/2))
	return values


# Expected input format
# dates [YYYY-MM-DD]
# times [HR:MIN] where HR in 24hr format
def make_corrected_epoch(dev_id, date_1, time_1, date_2, time_2):
	# Make lists of desired values
	info_1 = date_1.split('-')+time_1.split(':')
	info_2 = date_2.split('-')+time_2.split(':')

	# Convert times/dates into epoch time
	time1 = calendar.timegm((
		int(info_1[0]),
		int(info_1[1]),
		int(info_1[2]),
		int(info_1[3]),
		int(info_1[4]),
		0
	))

	time2 = calendar.timegm((
		int(info_2[0]),
		int(info_2[1]),
		int(info_2[2]),
		int(info_2[3]),
		int(info_2[4]),
		0
	))

	# Find UTC offset of device
	if os.getenv('SERVER_SOFTWARE', '').startswith('Google App Engine'):
		db = MySQLdb.connect(unix_socket='/cloudsql/bloomsky-backend-test:bloomsky', db='bloomsky', user='root')
	else:
		db = MySQLdb.connect(host='173.194.248.186', port=3306, user='wei', passwd='>lO9*iK,', db='bloomsky')
	cursor = db.cursor()
	cursor.execute('SELECT DISTINCT UTC FROM bloomsky.appi_skydevice WHERE DeviceID=\''+str(dev_id)+'\';')
	offset = int(cursor.fetchone()[0])
	db.close()

	# # Find UTC offset of device
	# conn = rethinkdb.connect(host='23.236.61.183',port=28015,db='bloomsky',auth_key='5Hx8X}Rx%6g`\rHkBXj+')
	# cursor = rethinkdb.table("skydata").filter({"DeviceID": dev_id}).order_by(rethinkdb.desc("TS")).limit(1).pluck("UTC").run(conn)
	# offset = int(cursor[0]["UTC"])
	# conn.close()

	# Correct time for UTC offset
	time1 = time1-(int(offset)*60*60)
	time2 = time2-(int(offset)*60*60)
	
	return [dev_id, time1, time2, int(offset)]

def db_query(data):
	f_names = [] # To keep track of file names

	# Connect to bloomsky-db (userdb)
	conn = rethinkdb.connect(host='23.236.61.183',port=28015,db='bloomsky',auth_key='5Hx8X}Rx%6g`\rHkBXj+')

	# Fetch all desired images
	urls = rethinkdb.table("skydata").between([data[0],data[1]],[data[0],data[2]],index="Device_TS").order_by(index='Device_TS').pluck('TS','ImageURL').run(conn)

	for url in urls:
		if 'ImageURL' in url:
			f_names.append(url['ImageURL'])
	
	# Close db connection and return list when done
	conn.close()
	return f_names

def pull_and_zip(names_list, data, path):
	# Format times for filenames
	time1 = data[1]
	time2 = data[2]
	time1 = time1+(data[3]*60*60)
	time2 = time2+(data[3]*60*60)
	t1 = time.strftime('%m-%d_%H-%M', time.gmtime(time1))
	t2 = time.strftime('%m-%d_%H-%M', time.gmtime(time2))

	# Set filenames
	subdir = data[0]+t1+'_to_'+t2
	zip_name = subdir+'.zip'

	temp = cStringIO.StringIO()							# Create in-memory storage
	zipzip = zipfile.ZipFile(temp, 'w')					# Write zip into memory

	# Pull and store in zip
	for name in names_list:
		decoded_name = decode('Il0veweather', str(name[-36:-4]))
		formatted_time = time.strftime('%m-%d-%Y_%H-%M-%S', time.gmtime(int(decoded_name[-10:])+(data[3]*60*60)))
		zipzip.writestr(os.path.join(subdir, data[0]+'_'+formatted_time+'.jpg'), urllib.urlopen(name).read())

	zipzip.extractall(path)
	zipzip.close()										# Must close zip to write all contents