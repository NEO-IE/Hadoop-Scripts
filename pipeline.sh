#the multir preprocessing pipeline

####################################################
#Define the number of mappers/reducers for different
#steps of the pipeline
MAPPER_STD=10
RED_STD=40
PP_MAPPER=$MAPPER_STD
PP_REDUCER=$RED_STD

PREPARSE_MAPPER_COUNT=$MAPPER_STD
PREPARSE_REDUCER_COUNT=$RED_STD

DEPPARSE_MAPPER_COUNT=$MAPPER_STD
CHUNKERMAPPER_COUNT=$MAPPER_STD
MARKER_MAPPER_COUNT=$MAPPER_STD
####################################################

####################################################
#Define the files to be used
FLATFILE=$1 #the initial flatfile
DOCIDFILE=$FLATFILE"IDS" #flat file with doc ids
PPOUTPUT=$FLATFILE"_PPED"   #preprocessing input
PREPARSEINPUT="$FLATFILE"_PPIN  #preparse processing input
PREPARSEOUTPUT="$FLATFILE"_PPOUT    #preparse processing output
PARSEINPUT=$FLATFILE"PARSEIN"   #parser input
PARSEOUT=$FLATFILE"PARSEOUT"    #parser output
DEPPARSEOUT=$FLATFILE"DEP"  #dependency parser output
CHUNKERINFILE="$FLATFILE"CHUNKIN    #chunker input
CHUNKEROUTFILE="$FLATFILE"CHUNKOUT  #chunker output
MARKER_INFILE=$PARSEINPUT   #marker input
MARKER_OUTFILE=$FLATFILE"MARKEROUT" #marker output
COMB_INCORRECT_ORDER="$FLATFILE"MERGE   #merged file 
COMB_CORRECT_ORDER="$FLATFILE"RES   #merged file with columns in correct order
####################################################
RUNSCRIPTPATH=runScripts

echo "Processing $FLATFILE"

echo "######################"
echo "Step1: Assign doc ids"
echo "######################"

LINES_PER_DOC=100
python assignDocIds.py "$FLATFILE" "$LINES_PER_DOC" > "$DOCIDFILE" #add document id to each line
echo -e "Assigning document ids to sentences, $LINES_PER_DOC sentences per doc\n\n"

echo "######################"
echo "Step2: feed to the multir preprocessor"
echo -e "######################\n\n"
echo "Starting input preprocessing"
hadoop fs -copyFromLocal $DOCIDFILE $DOCIDFILE
rm $DOCIDFILE
bash $RUNSCRIPTPATH/runMultirPreprocess.sh $DOCIDFILE $PPOUTPUT $PP_MAPPER $PP_REDUCER #add metadata required by stages ahead
echo "input preprocessing finished, copying files to disk, sorting and assigning sentence ids"
hadoop fs -getmerge $PPOUTPUT $PPOUTPUT
hadoop fs -rmr $PPOUTPUT
sed -i '/^\s*$/d' $PPOUTPUT
python assignsentids.py $PPOUTPUT > $PREPARSEINPUT  #assign a sentence id to each sentence
echo "Preparse input file created"

rm $PPOUTPUT $FLATFILEIDS

echo -e "\n\n######################"
echo "Step 3: Run Preparse processing"
echo -e "######################\n\n"

echo -e "Starting preparse processing, file: $PREPARSEINPUT"
echo "Copying file to HDFS"
hadoop fs -copyFromLocal $PREPARSEINPUT $PREPARSEINPUT
echo "Spawing the job"
bash $RUNSCRIPTPATH/runpre.sh  $PREPARSEINPUT $PREPARSEOUTPUT $PREPARSE_MAPPER_COUNT $PREPARSE_REDUCER_COUNT
echo "Copying results to disk, cleaning HDFS"
hadoop fs -rmr $PREPARSEINPUT
hadoop fs -getmerge $PREPARSEOUTPUT $PREPARSEOUTPUT
hadoop fs -rmr $PREPARSEOUTPUT

echo -e "\n\n######################"
echo "Step 4: Run Parsing"
echo -e "######################\n\n"

echo "starting parsing"
cut -f 1,4 $PREPARSEOUTPUT > $PARSEINPUT
hadoop fs -copyFromLocal $PARSEINPUT $PARSEINPUT
bash hadoopmultir/parsing/run.sh $PARSEINPUT $PARSEOUT
echo "Parsing complete, copying the results"
#hadoop fs -rmr $PARSEINPUT WILL BE USED FOR MARKER, DND
hadoop fs -getmerge $PARSEOUT $PARSEOUT

echo -e "\n\n######################"
echo "Step 5: Run Post Parse Processing (Dependency parsing: possibly slow, some tasks will fail, that's okay)"
echo -e "######################\n\n"
bash $RUNSCRIPTPATH/runPost.sh $PARSEOUT $DEPPARSEOUT $DEPPARSE_MAPPER_COUNT
echo "finished"
hadoop fs -getmerge $DEPPARSEOUT $DEPPARSEOUT
hadoop fs -rmr $DEPPARSEOUT
hadoop fs -rmr $PARSEOUT


echo -e "\n\n######################"
echo "Step 6: Run chunker"
echo -e "######################\n\n"

echo "Preparing input file to be fed to chunker"
join $PREPARSEINPUT $PREPARSEOUTPUT -t $'\t'|cut -f1,3,4,7 > $CHUNKERINFILE
echo "Copying the input file to HDFS"
hadoop fs -copyFromLocal $CHUNKERINFILE $CHUNKERINFILE
echo "Spawning the chunker"
bash $RUNSCRIPTPATH/runChunker.sh $CHUNKERINFILE $CHUNKEROUTFILE $CHUNKERMAPPER_COUNT
echo "Chunking over, cleaning up, copying files from HDFS"
hadoop fs -rmr $CHUNKERINFILE
hadoop fs -getmerge $CHUNKEROUTFILE $CHUNKEROUTFILE

echo -e "\n\n######################"
echo "Step 7: Run (country) Marker (make sure you apply your correct marker for this step)"
echo -e "######################\n\n"
echo "Spawning Marker"
bash $RUNSCRIPTPATH/runMarker.sh $MARKER_INFILE $MARKER_OUTFILE $MARKER_MAPPER_COUNT
hadoop fs -getmerge $MARKER_OUTFILE $MARKER_OUTFILE
echo "Marking over, getting data back to the disk"

echo -e "\n\n######################"
echo "Step 8: Combine all the results"
echo -e "######################\n\n"
echo "Sorting preparseoutput"
sort -nk1 $PREPARSEOUTPUT -o $PREPARSEOUTPUT
echo "Sorting chunkeroutput"
sort -nk1 $CHUNKEROUTFILE -o $CHUNKEROUTFILE
echo "Sorting marker output"
sort -nk1 $MARKER_OUTFILE -o $MARKER_OUTFILE
echo "Sorting dependency parse output"
sort -nk1 $DEPPARSEOUT -o $DEPPARSEOUT



join $PREPARSEINPUT $PREPARSEOUTPUT -t $'\t'|cut -f1,2,4-|join - $DEPPARSEOUT -t$'\t'|join - $CHUNKEROUTFILE -t $'\t'|join - $MARKER_OUTFILE -t $'\t' > $COMB_INCORRECT_ORDER
awk -F $'\t' 'BEGIN {OFS = FS} {print $1,$2,$8,$5,$4,$9,$11,$12,$7,$3,$6,$10}' $COMB_INCORRECT_ORDER > $COMB_CORRECT_ORDER
rm $COMB_INCORRECT_ORDER
echo "Finished!"
echo "Removing files"
hadoop fs -rmr $PREPARSEOUTPUT
hadoop fs -rm $PREPARSEINPUT
hadoop fs -rmr $CHUNKEROUTFILE
hadoop fs -rmr $MARKER_OUTFILE
hadoop fs -rmr $DEPPARSEOUT
mv $COMB_CORRECT_ORDER RES"$FLATFILE"
rm "$FLATFILE"[A-Z_]*
echo "Result written to: $COMB_CORRECT_ORDER"
