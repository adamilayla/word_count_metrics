spark-submit\
   --master local\ hdfs://TCP:IP/<PORT>
   --deploy-mode client\
   src/run.py $1 \ 

res=$?
echo "Job finished with status" $res
exit $res
