#!/bin/bash

function usage() {
    echo "Usage: ./run_csv_to_ipc.sh --user <user> --script-path <script_path> --in <input_csv_file> --out <output_ipc_file>"
    exit 1
}

conda_user="sen2agri-service"
script_path="csv_to_ipc.py"
input_file=""
output_file=""
int32_columns=""
float32_columns=""
bool_columns=""
text_columns=""
nullable_columns=""

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -u|--user)
    conda_user="$2"
    shift # past argument
    shift # past value
    ;;
    -p|--script-path)
    script_path="$2"
    shift # past argument
    shift # past value
    ;;
    -i|--in)
    input_file="$2"
    shift # past argument
    shift # past value
    ;;
    -o|--out)
    output_file="$2"
    shift # past argument
    shift # past value
    ;;

    -g|--int32-columns)
    int32_columns="$2"
    shift # past argument
    shift # past value
    ;;

    -f|--float-columns)
    float32_columns="$2"
    shift # past argument
    shift # past value
    ;;

    -b|--bool-columns)
    bool_columns="$2"
    shift # past argument
    shift # past value
    ;;

    -n|--nullable-columns)
    nullable_columns="$2"
    shift # past argument
    shift # past value
    ;;

    -t|--text-columns)
    text_columns="$2"
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


if [ -z ${script_path} ] ; then
    echo "No script-path provided!" && usage
else
    if [[ "${script_path}" = /* ]] ; then
        echo "${script_path} is absolute!"
    else
        path_to_executable=$(which ${script_path})
        if [ -x "$path_to_executable" ] ; then
            echo "${script_path} found in path!"
            script_path=${path_to_executable}
        else 
            # check if in the same directory as the sh script
            SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
            script_path1="${SCRIPT_DIR}/${script_path}"
            if [ ! -f ${script_path1} ] ; then 
                echo "Cannot find $script_path anywhere!"
                exit 1
            fi
            echo "${script_path1} found in the same folder with sh script (${SCRIPT_DIR}). Using it ..."
            script_path=${script_path1}
        fi
    fi
fi 

if [ -z ${input_file} ] ; then
    echo "No input file provided!" && usage
fi 
if [ -z ${output_file} ] ; then
    echo "No output file provided!" && usage
fi

INT32_COLS=""
FLOAT32_COLS=""
BOOL_COLS=""
NULLABLE_COLS=""
TEXT_COLS=""

if [ ! -z ${int32_columns} ] ; then
    INT32_COLS="--float-columns \"${int32_columns}\""
fi

if [ ! -z ${float32_columns} ] ; then
    FLOAT32_COLS="--int32-columns \"${float32_columns}\""
fi

if [ ! -z ${bool_columns} ] ; then
    BOOL_COLS="--bool-columns \"${bool_columns}\""
fi

if [ ! -z ${text_columns} ] ; then
    TEXT_COLS="--text-columns \"${text_columns}\""
fi

if [ ! -z ${nullable_columns} ] ; then
    NULLABLE_COLS="--nullable-columns \"${nullable_columns}\""
fi

if [ $USER == ${conda_user} ] ; then
    echo "Activating conda sen4cap for user $USER"
    CONDA_CMD="source ~/.bashrc && conda activate sen4cap"
    CMD_TERM=""
else 
    echo "Activating conda sen4cap from user $USER for user ${conda_user}"
    CONDA_CMD="sudo su -l ${conda_user} -c 'conda activate sen4cap"
    CMD_TERM="'"
fi    


PY_CMD="python ${script_path} --input ${input_file} --output ${output_file} ${INT32_COLS} ${FLOAT32_COLS} ${BOOL_COLS} ${TEXT_COLS} ${NULLABLE_COLS}"

CMD="${CONDA_CMD} && ${PY_CMD}"
CMD="${CMD}${CMD_TERM}"

echo "Executing ${CMD}"

#Execute the command
eval $CMD    
 