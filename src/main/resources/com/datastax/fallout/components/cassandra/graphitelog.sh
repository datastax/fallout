#!/bin/bash
# run any changes through https://www.shellcheck.net first

# Set this hostname
HOSTNAME=$(hostname --short)

if [ "$#" -lt 2 ]; then
    echo "Illegal number of parameters: $0 runName graphite_ip [graphite_port]"
    exit 1
fi

RUNNAME=$1

# Set Graphite host
GRAPHITE=$2
GRAPHITE_PORT=${3:-2003}
#            type,   total ops,  op/s,    pk/s,   row/s,    mean,     med,     .95,     .99,    .999,     max,   time,   stderr, errors,  gc: #,  max ms,  sum ms,  sdv ms,      mb
metricName=("opType" "totalOps" "opsPerSec" "primaryKeyPerSec" "rowsPerSec" "meanLatency" "medianLatency" "p95Latency" "p99Latency" "p999Latency" "maxLatency" "timeOffset" "errorRate" "exceptions" "gcCount" "maxGCTime" "sumGCTime" "stddevGC" "mbGCd")
metricLen=${#metricName[@]}

while read -r line
do
  echo "$line"
  DATE=$(date +%s)
  IFS=" " read -r -a linearray <<< "$(awk -F',' '{$1=$1} 1' <<< "$line")"

  len=${#linearray[@]}

  if [ "$metricLen" -eq "$len" ] ; then
      MSGTYPE=${linearray[0]}
      for (( i=1; i<len; i++ ));
      do
	  metric=${linearray[$i]}
	  re='^-?[0-9]+([.][0-9]+)?$'
	  if [[ "$metric" =~ $re ]] ; then
	      to_send="${RUNNAME}.${HOSTNAME}.${MSGTYPE}.${metricName[$i]} ${metric} ${DATE}"
	      # echo "$to_send"
	      echo "$to_send" | nc -q 0 "$GRAPHITE" "$GRAPHITE_PORT"
	  fi
      done
  fi
done < "/proc/${$}/fd/0"
