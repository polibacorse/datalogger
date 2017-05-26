import paho.mqtt.client as mqtt
from pathlib import Path
from datetime import date
import json
import time
import struct

ROOT_PATH = '/home/pi/PC5_log'
TOPICS = [
	'$SYS/formatted/datalog_on-off', 
	'$SYS/formatted/lap_finished', 
	'$SYS/raw'
]

button_pressed = False 
logs = {
	'general': None,
	'gps': None
}

path = None

def log(json_msg):
	msg = json.loads(json_msg)
	payload = struct.pack('<hq8i', msg['id'], msg['time'], *msg['data'])

	# check if message is from GPS
	# see Documentation for additional info
	if msg['id'] == 752 or msg['id'] == 753:
		logs['gps'].write(payload)
	else:
		logs['general'].write(payload)

def update_button_pressed(json_msg):
	msg = json.loads(json_msg)
	state = int(msg['data'])

	if state:
		button_pressed = True
	else:
		button_pressed = False

def update_lap_state(json_msg):
	msg = json.loads(json_msg)
	state = int(msg['data'])

	if state:
		tini_files()
		init_files(path)

def mqtt_on_connect(client, obj, flags, rc):
	for topic in TOPICS:
		client.subscribe(topic, 0)

def mqtt_on_message(client, obj, msg):
	payload_str = msg.payload.decode("utf-8")
	payload_str = payload_str.replace(",]}", "]}")	
	
	if msg.topic == TOPICS[0]:
		update_button_pressed(payload_str)
	elif msg.topic == TOPICS[1]:
		update_lap_state(payload_str)
	
	if button_pressed:
		log(payload_str)

def init_mqtt():
	client = mqtt.Client()
	client.on_connect = mqtt_on_connect
	client.on_message = mqtt_on_message
	client.connect_async('localhost')
	client.loop_start()

def files_get_index(path):
        flist = sorted(path.glob('*.dat'))
        if len(flist) != 0:
                last = flist[-1].split('_')
                idx = int(last[2][:1]) + 1
        else:
                idx = 0
        return idx

def init_files(path):
	idx = files_get_index(path);

	logs['general'] = open(str(path / 'PC5_log_{}.dat'.format(idx)), 'wb')
	logs['gps'] = open(str(path / 'PC5_log_{}_gps.dat'.format(idx)), 'wb')

def init_path(path_str):
	path = Path(path_str)
	if not path.is_dir():
		path.mkdir()

	today = date.today()
	# /<dd-mm-yy>/
	path /= '{:%d-%m-%y}'.format(today)
	if not path.is_dir():
		path.mkdir()

	# /session_<num_session>
	last_child = 0
	curr_child = 0
	for child in path.iterdir():
		if child.is_dir():
			curr_child = int(child.name[-2:])
			if curr_child > last_child:
				last_child = int(child.name[-2:])

	path /= 'session_{:02d}'.format(last_child +1)
	path.mkdir()
	return path

def tini_files():
	for log in logs:
		try:
			log.close()
		except:
			pass

def main():
	global path
	path = init_path(ROOT_PATH)
	init_files(path)
	init_mqtt()

	while(True):
		time.sleep(1)

if __name__ == '__main__':
	try:
		main()
	except KeyboardInterrupt:
		tini_files()
		exit(0)
