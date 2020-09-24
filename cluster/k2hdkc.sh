#!/bin/sh
#
# k2hdkc_go
#
# Copyright 2018 Yahoo Japan Corporation.
#
# Go driver for k2hdkc that is a highly available and scalable distributed
# KVS clustering system. For k2hdkc, see
# https://github.com/yahoojapan/k2hdkc for the details.
#
# For the full copyright and license information, please view
# the license file that was distributed with this source code.
#
# AUTHOR:   Hirotaka Wakabayashi
# CREATE:   Fri, 14 Sep 2018
# REVISION:
#

# A K2hdkc Cluster management script.

USER=$(whoami)
BASEDIR=$(dirname "$0")

start_process() {
	echo "chmpx -conf ${BASEDIR}/server.yaml -d dump > ${BASEDIR}/chmpx.log 2>&1"
	nohup chmpx -conf ${BASEDIR}/server.yaml -d dump > ${BASEDIR}/chmpx.log 2>&1 &
	echo "sleep 3"
	sleep 3
	echo "k2hdkc -conf ${BASEDIR}/server.yaml -d dump > ${BASEDIR}/k2hdkc.log 2>&1"
	nohup k2hdkc -conf ${BASEDIR}/server.yaml -d dump > ${BASEDIR}/k2hdkc.log 2>&1 &
	echo "sleep 3"
	sleep 3
	echo "chmpx -conf ${BASEDIR}/slave.yaml -d dump  > ${BASEDIR}/slave.log 2>&1"
	nohup chmpx -conf ${BASEDIR}/slave.yaml -d dump  > ${BASEDIR}/slave.log 2>&1 &
	echo "sleep 3"
	sleep 3
}

stop_process() {
	for PROC in chmpx k2hdkc; do
		echo "pgrep -u ${USER} -x ${PROC}"
		pgrep -u ${USER} -x ${PROC}
		if test "${?}" = "0"; then
			echo "pkill -x ${PROC}"
			pkill -x ${PROC}
		fi
	done
}

status_process() {
	RET=0
	for PROC in chmpx k2hdkc; do
		echo "pgrep -u ${USER} -x ${PROC}"
		pgrep -u ${USER} -x ${PROC}
		RET=$(echo ${?} + ${RET}|bc)
	done
	return ${RET}
}

case $1 in
startifnotexist)
	status_process
	if test "${RET}" = "2" ; then
		echo "start_process"
		start_process
	elif test "${RET}" = "1" ; then
		echo "stop_process"
		stop_process
		echo "start_process"
		start_process
	fi
	;;
start)
	start_process
	;;
stop)
	stop_process
	;;
status)
	status_process
	;;
*)
	echo "$0 (start|stop|status|startifnotexist)"
	exit 1
	;;
esac
exit 0

# Local Variables:
# c-basic-offset: 4
# tab-width: 4
# indent-tabs-mode: t
# End:
# vim600: noexpandtab sw=4 ts=4 fdm=marker
# vim<600: noexpandtab sw=4 ts=4

