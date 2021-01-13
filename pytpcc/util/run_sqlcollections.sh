Url=${1:-127.0.0.1:8093}
Site=http://$Url/query/service
while read line;
do
sql1=$line
qc="&query_context=default:default.tpcc"
sql="${sql1} ${qc}"
echo curl -u Administrator:password -v $Site  -d statement="$sql"
curl -u Administrator:password -v $Site  -d statement="$sql" 
done
