KHOME=/opt/kafka
KHOME_BIN="$KHOME/bin"
ZOOKEPEER="localhost:2181"


BROKER_LIST=
TOPICS_FILE=



# ---------------------- usage ---------------------
function usage() {
  cat << __USAGE__ >&2

Usage ${0} -d [ -bt ]

Options:

   -b   list of brokers separated by comma
   -t   file with topics

__USAGE__
} 


function get_long_options(){
   local OPTIONS=$@
   local ARGUMENTS=($OPTIONS)
   local index=0

   for ARG in $OPTIONS; do
       index=$(($index+1));
       case $ARG in
         --brokers-list|-b) BROKER_LIST="${ARGUMENTS[index]}";;
         --topics|-t) TOPICS_FILE="${ARGUMENTS[index]}";;
         --help|-h) usage; exit 0;;
      esac
   done
}


function test_options(){
   if [ ! -f "$TOPICS_FILE" ]; then
      echo "File with topics not found: $TOPICS_FILE"
      exit 1
   fi

   [ -z "$BROKER_LIST" ] && BROKER_LIST="localhost:9092"
}



# ---------------------- funcs ---------------------
function get_offset(){
   echo "$1" | cut -d ':' -f3
}


function error_log(){
   echo "ERR: $1" 1>&1
}


function do_alert(){
   error_log "doing some alert for topic '$1' here..."
}


# ---------------------- MAIN ---------------------
get_long_options "$@"

test_options


LIST_OF_TOPICS="$(cat $TOPICS_FILE)"

echo "$LIST_OF_TOPICS" | while read LINE; do
   TOPIC=$(echo $LINE | cut -d ':' -f1)
   DATA=$(echo $LINE | cut -s -d':' -f2-)

   IS_EXISTS="$($KHOME_BIN/kafka-topics.sh --list --zookeeper $ZOOKEPEER --topic $TOPIC)"
   if [ ! -z "$IS_EXISTS" ]; then
      TOPIC_STATE=$($KHOME_BIN/kafka-run-class.sh kafka.tools.GetOffsetShell --topic $TOPIC --broker-list $BROKER_LIST --time -1)
      if [ -z "$DATA" ]; then
         sed -i "s/^$TOPIC\$/$TOPIC_STATE/g" "$TOPICS_FILE"
      elif [ $(get_offset $TOPIC_STATE) -gt $(get_offset $LINE) ]; then
         sed -i "s/^$LINE/$TOPIC_STATE/g" "$TOPICS_FILE"
         grep $TOPIC_PRV_STATE "$OFFSET_FILE"
      else
         do_alert $TOPIC $TOPIC_STATE
      fi
   else
      error_log "Topic '$TOPIC' not found" 
   fi

done



exit 0
