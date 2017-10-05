#!/bin/bash

EXPORTER_URL=$1
AUTH=$2
UUID_LIST=$3

postBody=""
if [ -n "${UUID_LIST}" ]; then
postBody="{\"ids\":\"${UUID_LIST}\"}"
fi
jobResult=`curl -qSfs "${EXPORTER_URL}/export" -H "Authorization: ${AUTH}" -XPOST -d "${postBody}" 2>/dev/null`
if [ "$?" -ne 0 ]; then
  echo ">>Exporter service cannot be called successfully. Full export failed"
  exit 1
else
  jobID=`echo "${jobResult}" | jq '.ID' | cut -d'"' -f2 2>/dev/null`
  echo "FULL export initiated. Job id: ${jobID}"
  sleep 3
  status="Running"
  while [ ${status} != "Finished" ]; do
  job=`curl -qSfs "${EXPORTER_URL}/jobs/${jobID}" -H "Authorization: ${AUTH}" 2>/dev/null`

  if [ "$?" -ne 0 ]; then
	echo ">>Failed to retrieve job"
	exit 1
  else
	 status=`echo ${job} | jq '.Status' | cut -d'"' -f2 2>/dev/null`
  fi

  echo ${job}
  sleep 3
  done
  echo "FULL export finished. Check logs if there are failures"
fi
