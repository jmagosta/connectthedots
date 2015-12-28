# code from https://azure.microsoft.com/en-us/documentation/articles/service-bus-python-how-to-use-queues/
#!/usr/bin/python
# 14 Dec 2015  JMA
# servicebus.py
# Post live data from the TED5000 to Azure service bus
"""
Collect voltage and wattage data from the TED api. 
And send each reading when read to Azure.


"""
import pprint, os, sys
from time import sleep, time
import argparse
from json import dumps
from azure.servicebus import ServiceBusService, Message, Queue
import poll_ted as pt

dbg = False

# EVENT HUB URL
EVENTHUB_NAME = 'tedeventhub'
EVENTHUB_HOST = 'tednamespace.servicebus.windows.net'
EVENTHUB_URL='https://' + EVENTHUB_HOST + '/' + EVENTHUB_NAME
PRIMARY_KEY = '<YOUR_KEY>'
POLICY_NAME = 'tedsendpolicy'
CONNECTION_STRING = 'Endpoint=<YOURENDPOINT>'
ISSUER = ''
REPETITIONS = 3600
PERIOD = 'second'

#####################################################################
def bus_setup():
    # Create a ServiceBusService object.
    # Replace mynamespace, sharedaccesskeyname, and sharedaccesskey 
    # with your namespace, shared access signature (SAS) key name, and value.
    bus_service = ServiceBusService('TEDnamespace', shared_access_key_name=POLICY_NAME, shared_access_key_value=PRIMARY_KEY)
    return bus_service


########################################################################
def convert_to_dict(reading):
        # Express the readings as a labeled dictionary.  In the future, do this instead
        # in poll_ted.parse_ted_history_xml
        return {"sensed":reading[0], "watts":reading[1], "decivolts":reading[2]}


########################################################################
# Write the output to a tab-delimited file, 
# And return the output as a list of lists
def read_ted(period, repetitions, out_file='ted500.out'):
    uri = pt.request_uri(period)
    bus = bus_setup()
    extract_seq = [None]
    with open(out_file, 'wb') as out_fd:
        print 'Begun polling.', out_file
        for reading, extract_seq in pt.iterate_readings(period, uri, extract_seq, repetitions):
            # if period == 'second':
            #     sleep(0.9)
            # else:
            #     sleep(59)
            sleep(float(period) - 0.1)
            rec = dumps(convert_to_dict(reading))
            extract_seq.append(rec)
            pt.wr_list(out_fd, reading)
            try:
                bus.send_event(EVENTHUB_NAME, rec)
            except:
                print 'send_event failed for ', rec
    return extract_seq[1:]

#####################################################################
def options():
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('interval', nargs='?', default=1,
                            help='Number of seconds between checks for readings. An integer > 0.')
        parser.add_argument('-v', action='store_true',
                            help='Print verbose output of intermediate results.')
        return parser.parse_args()

        
#####################################################################
def main(intv):
    return read_ted(intv, REPETITIONS)


########################################################################
if __name__ == '__main__':
    ## If invoked with no args, just print the usage string
    args = options()
    dbg = args.v
    pt.dbg = dbg

    #REPETITIONS = int(args.interval)
    start = time()
    pprint.pprint(main(args.interval))
    print >> sys.stderr, dbg, sys.argv, "Done in ", '%5.3f' % (time() - start), " secs!"


### (c) 2015 John M Agosta


