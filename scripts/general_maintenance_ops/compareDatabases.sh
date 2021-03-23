#!/bin/sh

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --db1)
    DB1_NAME="$2"
    shift # past argument
    shift # past value
    ;;
    --db2)
    DB2_NAME="$2"
    shift # past argument
    shift # past value
    ;;
    -w|--working-dir)
    WORKING_DIR="$2"
    shift # past argument
    shift # past value
    ;;
    -o|--out)
    OUT_FILE="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [ -z ${DB1_NAME} ] ; then 
    echo "Please provide --db1 parameter!"
    exit 1
fi
if [ -z ${DB2_NAME} ] ; then 
    echo "Please provide --db2 parameter!"
    exit 1
fi
if [ -z ${WORKING_DIR} ] ; then 
    echo "Please provide --working-dir parameter!"
    exit 1
fi
if [ -z ${OUT_FILE} ] ; then 
    echo "Please provide --out parameter!"
    exit 1
fi


function generate_files()
{
  DB_NAME=$1
  
  # extract name, schema name for all tables from database
  psql -U postgres $DB_NAME -At --field-separator ' ' -c " SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema in ('public','reports') AND table_type = 'BASE TABLE' order by table_name"\
  | while read -ra Record ; do
        name=${Record[0]}
        schema=${Record[1]}
        
	echo extract table : $name from $schema for database $DB_NAME
        # dump the schema for all the tables
        if [ "$schema" != "public" ]; then
              nameWithSchema=$schema.$name
              pg_dump -U postgres $DB_NAME --schema-only -t  $nameWithSchema > $WORKING_DIR/$DB_NAME/tables/$name.txt
        else
              pg_dump -U postgres $DB_NAME --schema-only -t  $name > $WORKING_DIR/$DB_NAME/tables/$name.txt
        fi
    
        case $name in
        	product_details_l4a|product_details_l4c)
                	INTEROGATION="SELECT * FROM $name order by 1"
                ;;
		user)
                	INTEROGATION="SELECT * FROM \"$name\" order by 1"			
		;;
                *) 
 		        # get data for every table, except columns that have the type as timestamp
                	if [ "$schema" != "public" ]; then
                   		nameWithSchema=$schema.$name
                   		INTEROGATION="$(psql -AtU postgres $DB_NAME -c "SELECT 'SELECT ' || STRING_AGG('o.' || column_name, ',') || ' FROM $nameWithSchema AS o  ORDER BY 1' 
		        		FROM information_schema.columns WHERE table_name='$name' AND data_type NOT LIKE '%timestamp%'")"
                	else
		   		INTEROGATION="$(psql -AtU postgres $DB_NAME -c "SELECT 'SELECT ' || STRING_AGG('o.' || column_name, ',') || ' FROM $name AS o  ORDER BY 1' 
		   		FROM information_schema.columns WHERE table_name='$name' AND data_type NOT LIKE '%timestamp%'")"
                	fi
                ;;
	 esac
	 psql -U postgres $DB_NAME -c "$INTEROGATION" --csv > $WORKING_DIR/$DB_NAME/tables/$name.csv
    done

  # extract name from database for all the functions
  
  function_name="$(psql -U postgres $DB_NAME -t -c  "SELECT f.proname FROM pg_catalog.pg_proc f INNER JOIN pg_catalog.pg_namespace n
  ON (f.pronamespace = n.oid) WHERE n.nspname in ('public','reports') AND f.proname LIKE 'sp\_%' escape '\'")"
  for line in $function_name 
  do
        # extract all functions in a separate csv file
        echo extract function : $line for database $DB_NAME
	psql -AtU postgres $DB_NAME -c "SELECT pg_get_functiondef(oid) FROM pg_catalog.pg_proc WHERE proname='$line'" > $WORKING_DIR/$DB_NAME/functions//$line.txt
  done
}

mkdir -p $DB1_NAME/{tables,functions}
mkdir -p $DB2_NAME/{tables,functions}
generate_files "$DB1_NAME"
generate_files "$DB2_NAME"

# compare the two database

# compare files between $DB1_NAME database and $DB2_NAME database
diff -q -w -B -r $DB1_NAME $DB2_NAME > $OUT_FILE
