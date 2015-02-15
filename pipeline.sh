#the multir preprocessing pipeline
FLATFILE=$1 #the initial flatfile

echo "Processing $FLATFILE"

echo "######################"
echo "Step1: Assign doc ids"
echo "######################"

LINES_PER_DOC=100
DOCIDFILE=$FLATFILE"IDS"
python assignDocIds.py "$FLATFILE" "$LINES_PER_DOC" > "$DOCIDFILE"
echo -e "Assigning document ids to sentences, $LINES_PER_DOC sentences per doc\n\n"

echo "######################"
echo "Step2: feed to the multir preprocessor"
echo -e "######################\n\n"
PP_MAPPER=1
PP_REDUCER=1
echo "Starting input preprocessing"
hadoop fs -copyFromLocal $DOCIDFILE $DOCIDFILE
rm $DOCIDFILE
PPOUTPUT=$FLATFILE"_PPED"
bash runMultirPreprocess.sh $DOCIDFILE $PPOUTPUT $PP_MAPPER $PP_REDUCER
echo "input preprocessing finished, copying files to disk, sorting and assigning sentence ids"
hadoop fs -getmerge $PPOUTPUT $PPOUTPUT
hadoop fs -rmr $PPOUTPUT
sed -i '/^\s*$/d' $PPOUTPUT
PREPARSEINPUT="$FLATFILE"_PPIN
python assignsentids.py $PPOUTPUT > $PREPARSEINPUT
echo "Preparse input file created"

rm $PPOUTPUT $FLATFILEIDS

echo -e "\n\n######################"
echo "Step 3: Run Preparse processing"
echo -e "######################\n\n"

echo -e "Starting preparse processing, file: $PREPARSEINPUT"
echo "Copying file to HDFS"
hadoop fs -copyFromLocal $PREPARSEINPUT $PREPARSEINPUT
PREPARSEOUTPUT="$FLATFILE"_PPOUT
PREPARSE_MAPPER_COUNT=1
PREPARSE_REDUCER_COUNT=1
echo "Spawing the job"
bash runpre.sh  $PREPARSEINPUT $PREPARSEOUTPUT $PREPARSE_MAPPER_COUNT $PREPARSE_REDUCER_COUNT
echo "Copying results to disk, cleaning HDFS"
hadoop fs -rmr $PREPARSEINPUT
hadoop fs -getmerge $PREPARSEOUTPUT $PREPARSEOUTPUT
hadoop fs -rmr $PREPARSEOUTPUT

echo -e "\n\n######################"
echo "Step 4: Run Parsing"
echo -e "######################\n\n"

echo "starting parsing"
PARSEINPUT=$FLATFILE"PARSEIN"
PARSEOUT=$FLATFILE"PARSEOUT"
cut -f 1,4 $PREPARSEOUTPUT > $PARSEINPUT
hadoop fs -copyFromLocal $PARSEINPUT $PARSEINPUT
bash hadoopmultir/parsing/run.sh $PARSEINPUT $PARSEOUT
echo "Parsing complete, copying the results"
hadoop fs -rmr $PARSEINPUT
hadoop fs -getmerge $PARSEOUT $PARSEOUT

echo -e "\n\n######################"
echo "Step 5: Run Post Parse Processing (Dependency parsing: possibly slow, some tasks will fail, that's okay)"
echo -e "######################\n\n"
DEPPARSEOUT=$FLATFILE"DEP"
DEPPARSE_MAPPER_COUNT=1
bash runPost.sh $PARSEOUT $DEPPARSEOUT $DEPPARSE_MAPPER_COUNT
echo "finished"
hadoop fs -getmerge $DEPPARSEOUT $DEPPARSEOUT
hadoop fs -rmr $DEPPARSEOUT
hadoop fs -rmr $PARSEOUT



