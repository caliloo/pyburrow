#!/usr/bin/env python3

import subprocess
import re
import os.path
import sys
import time
import argparse
import yaml

parser = argparse.ArgumentParser(description='Get consumer groups infos from a kafka cluster.')
parser.add_argument('config_file', default='config.yml' ,help="path to the config file")

args = parser.parse_args()


from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

config = yaml.load(open(args.config_file ,'r').read(), Loader)


print(config)

s = subprocess.run(["ssh", 
	"%s" % config['general']['remote-ssh'] ,
	"export PATH=%s:$PATH; kafka-consumer-groups.sh --bootstrap-server %s --list" % (config['general']['kafka-bin-path'],config['general']['bootstrap-server'])],
	stdout=subprocess.PIPE,
	stderr=subprocess.PIPE
	)

lines = s.stdout.decode().split('\n')



from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI
influx_token = config['general']['influxdb']['token']
influx_org = config['general']['influxdb']['org']
influx_bucket = config['general']['influxdb']['bucket']
influx_server = config['general']['influxdb']['server']

while True:
	with InfluxDBClient(url=influx_server, token=influx_token, org=influx_org) as client:

		write_api = client.write_api(write_options=SYNCHRONOUS)
		data = []




		for l in lines[:-1] :
			if config['general']['exclude'] is not None :
				for exclude_pattern in config['general']['exclude'] :
					if re.match(exclude_pattern,l) is not None :
						print("skipping", l, "because of pattern", exclude_pattern)
						continue
					



			group = l
			result = subprocess.run(["ssh", 
				"%s" % config['general']['remote-ssh'] ,
				"export PATH=%s:$PATH; kafka-consumer-groups.sh --bootstrap-server %s --describe --group %s" % (config['general']['kafka-bin-path'],config['general']['bootstrap-server'],group)],
				stdout=subprocess.PIPE,
				stderr=subprocess.PIPE
				)	

			for l in result.stdout.decode().split('\n')[2:-1] :
				l = re.sub(' +', ' ', l)
				topic, partition, current_offset, end_offset, lag, consumer_id, host, client_id = l.split()

				partition = int(partition)
				current_offset = None if current_offset == '-' else int(current_offset)
				end_offset = int(end_offset)
				lag = None if lag == '-' else int(lag)


				head = "stream_infos,group=%s,topic=%s,partition=%u,client_id=%s" % (
					group, topic, partition, client_id)

				fields=[]
				if current_offset is not None :
					fields.append("current_offset=%u" % current_offset)
				fields.append("end_offset=%u" % end_offset)
				if lag is not None :
					fields.append("lag=%u" % lag)
				fields.append("client_id=\"%s\"" % client_id)

				field = ",".join(fields)


				data.append("%s %s" % (head, field))


		write_api.write(influx_bucket, influx_org, "\n".join(data))
	print(".", flush=True, end='')
	time.sleep(30)
