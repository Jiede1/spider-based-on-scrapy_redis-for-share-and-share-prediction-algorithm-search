#!/usr/bin/python

# ----------------------------------------------------------------------
# Numenta Platform for Intelligent Computing (NuPIC)
# Copyright (C) 2013, Numenta, Inc.  Unless you have an agreement
# with Numenta, Inc., for a separate license for this software code, the
# following terms and conditions apply:
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
# http://numenta.org/licenses/
# ----------------------------------------------------------------------
import csv
from nupic.frameworks.opf.model_factory import ModelFactory
from nupic_output import NuPICFileOutput, NuPICPlotOutput
from nupic.swarming import permutations_runner


# Change this to switch from a matplotlib plot to file output.
PLOT = False
SWARM_CONFIG = {
  "includedFields": [
    {
      "fieldName": "value",
      "fieldType": "float",
      "maxValue": 1.0,
      "minValue": 0.0
    }
  ],
  "streamDef": {
    "info": "value",
    "version": 1,
    "streams": [
      {
        "info": "choice.csv",
        "source": "file://home/sdbadmin/runSwarm/choice.csv",
        "columns": [
          "*"
        ]
      }
    ]
  },
  "inferenceType": "TemporalAnomaly",
  "inferenceArgs": {
    "predictionSteps": [
      3
    ],
    "predictedField": "value"
  },
  "swarmSize": "medium"
}



def swarm_over_data(filename):
    config=SWARM_CONFIG
    print('filename: ',filename)
    
    print config['streamDef']['streams']
    config['streamDef']['streams'][0]['info']=filename
    config['streamDef']['streams'][0]['source']="file://runSwarm/"+filename
    return permutations_runner.runWithConfig(config,
        {'maxWorkers': 4, 'overwrite': True})



def run_sine_experiment():
  input_file = "sine.csv"
  generate_data.run(input_file)
  model_params = swarm_over_data()
  if PLOT:
    output = NuPICPlotOutput("sine_output", show_anomaly_score=True)
  else:
    output = NuPICFileOutput("sine_output", show_anomaly_score=True)
  model = ModelFactory.create(model_params)
  model.enableInference({"predictedField": "sine"})

  with open(input_file, "rb") as sine_input:
    csv_reader = csv.reader(sine_input)

    # skip header rows
    csv_reader.next()
    csv_reader.next()
    csv_reader.next()

    # the real data
    for row in csv_reader:
      angle = float(row[0])
      sine_value = float(row[1])
      result = model.run({"sine": sine_value})
      output.write(angle, sine_value, result, prediction_step=1)

  output.close()

def generate_data(a,filename):
    print "Generating data into %s" % filename
    fileHandle = open('/home/sdbadmin/runSwarm/'+filename,"w")
    writer = csv.writer(fileHandle)
    writer.writerow(["Time","value"])
    writer.writerow(["int","float"])
    writer.writerow(["",""])

    for i in range(len(a)):
	time=i
	value=a[i]
        writer.writerow([time, value])

    fileHandle.close()
    print "Generated %i rows of output data into %s" % (len(a), filename)


def swarm(a,number,col):
    generate_data(a,str(col)+'.csv')
    model_params = swarm_over_data(filename=str(col)+'.csv')

    '''model params save'''
    import json
    fp=file('/home/sdbadmin/'+str(col)+'_swarmParams.csv','w')
    json.dump(model_params,fp)
    fp.close()

    if PLOT:
        output = NuPICPlotOutput(str(col)+"_swarm__output", show_anomaly_score=True)
    else:
        output = NuPICFileOutput(str(col)+"_swarm_output", show_anomaly_score=True)
    model = ModelFactory.create(model_params)
    model.enableInference({"predictedField": "value"})
    
    input_file='/home/sdbadmin/runSwarm/'+str(col)+'.csv'
    with open(input_file, "rb") as sine_input:
        csv_reader = csv.reader(sine_input)
        # the real data

	# skip header rows
        csv_reader.next()
        csv_reader.next()
        csv_reader.next()

        for row in csv_reader:
            time=float(row[0])
            value = float(row[1])
            result = model.run({"value": value})
            output.write(time,value, result, prediction_step=3)

    output.close()
    return model_params
