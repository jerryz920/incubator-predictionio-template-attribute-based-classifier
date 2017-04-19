"""
Import sample data for classification engine
"""

import predictionio
import argparse
import time

def batch_import_events(client, file):
  print file
  f = open(file, 'r')

  count = 0
  create_event_request_list = []

  old_time = time.time()
  new_time = 0.0
  start_time = old_time

  print "Importing data from",file,"..."
 
  for line in f:
    data = line.rstrip('\r\n').split(",")
    year = data[0]
    attr = data[1:90]
    req = client.acreate_event( #async method
      event="$set",
      entity_type="user",
      entity_id=str(count), # use the count num as user ID
      # Import 6 data for test
      properties= {
        "MSD_0" : float(attr[0]) + 15000.0,
        "MSD_1" : float(attr[1]) + 15000.0,
        "MSD_2" : float(attr[2]) + 15000.0,
        "MSD_3" : float(attr[3]) + 15000.0,
        "MSD_4" : float(attr[4]) + 15000.0,
        "MSD_5" : float(attr[5]) + 15000.0,
        "MSD_6" : float(attr[6]) + 15000.0,
        "MSD_7" : float(attr[7]) + 15000.0,
        "MSD_8" : float(attr[8]) + 15000.0,
        "MSD_9" : float(attr[9]) + 15000.0,
        "MSD_10" : float(attr[10]) + 15000.0,
        "MSD_11" : float(attr[11]) + 15000.0,
        "MSD_12" : float(attr[12]) + 15000.0,
        "MSD_13" : float(attr[13]) + 15000.0,
        "MSD_14" : float(attr[14]) + 15000.0,
        "MSD_15" : float(attr[15]) + 15000.0,
        "MSD_16" : float(attr[16]) + 15000.0,
        "MSD_17" : float(attr[17]) + 15000.0,
        "MSD_18" : float(attr[18]) + 15000.0,
        "MSD_19" : float(attr[19]) + 15000.0,
        "MSD_20" : float(attr[20]) + 15000.0,
        "MSD_21" : float(attr[21]) + 15000.0,
        "MSD_22" : float(attr[22]) + 15000.0,
        "MSD_23" : float(attr[23]) + 15000.0,
        "MSD_24" : float(attr[24]) + 15000.0,
        "MSD_25" : float(attr[25]) + 15000.0,
        "MSD_26" : float(attr[26]) + 15000.0,
        "MSD_27" : float(attr[27]) + 15000.0,
        "MSD_28" : float(attr[28]) + 15000.0,
        "MSD_29" : float(attr[29]) + 15000.0,
        "MSD_30" : float(attr[30]) + 15000.0,
        "MSD_31" : float(attr[31]) + 15000.0,
        "MSD_32" : float(attr[32]) + 15000.0,
        "MSD_33" : float(attr[33]) + 15000.0,
        "MSD_34" : float(attr[34]) + 15000.0,
        "MSD_35" : float(attr[35]) + 15000.0,
        "MSD_36" : float(attr[36]) + 15000.0,
        "MSD_37" : float(attr[37]) + 15000.0,
        "MSD_38" : float(attr[38]) + 15000.0,
        "MSD_39" : float(attr[39]) + 15000.0,
        "MSD_40" : float(attr[40]) + 15000.0,
        "MSD_41" : float(attr[41]) + 15000.0,
        "MSD_42" : float(attr[42]) + 15000.0,
        "MSD_43" : float(attr[43]) + 15000.0,
        "MSD_44" : float(attr[44]) + 15000.0,
        "MSD_45" : float(attr[45]) + 15000.0,
        "MSD_46" : float(attr[46]) + 15000.0,
        "MSD_47" : float(attr[47]) + 15000.0,
        "MSD_48" : float(attr[48]) + 15000.0,
        "MSD_49" : float(attr[49]) + 15000.0,
        "MSD_50" : float(attr[50]) + 15000.0,
        "MSD_51" : float(attr[51]) + 15000.0,
        "MSD_52" : float(attr[52]) + 15000.0,
        "MSD_53" : float(attr[53]) + 15000.0,
        "MSD_54" : float(attr[54]) + 15000.0,
        "MSD_55" : float(attr[55]) + 15000.0,
        "MSD_56" : float(attr[56]) + 15000.0,
        "MSD_57" : float(attr[57]) + 15000.0,
        "MSD_58" : float(attr[58]) + 15000.0,
        "MSD_59" : float(attr[59]) + 15000.0,
        "MSD_60" : float(attr[60]) + 15000.0,
        "MSD_61" : float(attr[61]) + 15000.0,
        "MSD_62" : float(attr[62]) + 15000.0,
        "MSD_63" : float(attr[63]) + 15000.0,
        "MSD_64" : float(attr[64]) + 15000.0,
        "MSD_65" : float(attr[65]) + 15000.0,
        "MSD_66" : float(attr[66]) + 15000.0,
        "MSD_67" : float(attr[67]) + 15000.0,
        "MSD_68" : float(attr[68]) + 15000.0,
        "MSD_69" : float(attr[69]) + 15000.0,
        "MSD_70" : float(attr[70]) + 15000.0,
        "MSD_71" : float(attr[71]) + 15000.0,
        "MSD_72" : float(attr[72]) + 15000.0,
        "MSD_73" : float(attr[73]) + 15000.0,
        "MSD_74" : float(attr[74]) + 15000.0,
        "MSD_75" : float(attr[75]) + 15000.0,
        "MSD_76" : float(attr[76]) + 15000.0,
        "MSD_77" : float(attr[77]) + 15000.0,
        "MSD_78" : float(attr[78]) + 15000.0,
        "MSD_79" : float(attr[79]) + 15000.0,
        "MSD_80" : float(attr[80]) + 15000.0,
        "MSD_81" : float(attr[81]) + 15000.0,
        "MSD_82" : float(attr[82]) + 15000.0,
        "MSD_83" : float(attr[83]) + 15000.0,
        "MSD_84" : float(attr[84]) + 15000.0,
        "MSD_85" : float(attr[85]) + 15000.0,
        "MSD_86" : float(attr[86]) + 15000.0,
        "MSD_87" : float(attr[87]) + 15000.0,
        "MSD_88" : float(attr[88]) + 15000.0,
        "year" : float(year)
      }
    )
    count += 1
    create_event_request_list.append(req)
    if count % 10 == 0:
      [r.get_response() for r in create_event_request_list]
      new_time = time.time()        
      speed = 10.0 / (new_time-old_time)
      old_time = new_time
      used_time = new_time - start_time
      m, s = divmod(used_time, 60)
      h, m = divmod(m, 60)
      print count,"events are imported. Speed:", "%.2f inst/sec" % speed, ". Used time: %d:%02d:%02d" % (h, m, s),"\r",
      create_event_request_list[:] = []
    
  f.close()
  print "\n%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for classification engine")
  parser.add_argument('--access_key', default='WgnqOUVALRhgAlQICWq6NehJQxNKVzzfacQs5NMYOdIKrJIwXfF9HsNdUM7jp8Pe')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="plain-train.txt")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  batch_import_events(client, args.file)
