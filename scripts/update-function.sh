#!/bin/bash

#------------------------------------------------------------------------------
#
# AUTHOR:  Blake Huber
#
# SYNOPSIS:
#   - script to update AWS lambda code packages and any associated
#     aliases to point to the new code revision
#
# REQUIREMENTS:
#   - awscli
#   - jq (json parser)
#   - ubuntu linux v14.04 or higher
#   - IAM roles in each account
#
# LICENSE:
#   - GPL v3
#------------------------------------------------------------------------------

# global vars
pkg=$(basename $0)
ALIAS='PROD'                        # name of function alias to update
REGION='eu-west-1'                  # region in which cw event is located
IAM_PROFILE='atos-aua1'             # profile in your local awscli config used to assume roles
LAMBDA_NAME_KEYWORD='spotprice'     # identifying keyword in the name of the lambda function
declare -a ARR_ACCOUNTS
# error codes
E_BADARG=8                      # exit code if bad input parameter

# Formatting
blue=$(tput setaf 4)
cyan=$(tput setaf 6)
green=$(tput setaf 2)
purple=$(tput setaf 5)
red=$(tput setaf 1)
white=$(tput setaf 7)
yellow=$(tput setaf 3)
gray=$(tput setaf 008)
lgray='\033[0;37m'              # light gray
dgray='\033[1;30m'              # dark gray
reset=$(tput sgr0)
#
BOLD=`tput bold`
UNBOLD=`tput sgr0`
title=$(echo -e ${white}${BOLD})
bodytext=${reset}

#
# system functions  ------------------------------------------------------
#

# indent
indent02() { sed 's/^/  /'; }
indent04() { sed 's/^/    /'; }
indent10() { sed 's/^/          /'; }

help_menu(){
    cat <<EOM

                        Help Contents
                        -------------

  ${title}SYNOPSIS${bodytext}

            $  sh ${BOLD}update-function.sh${UNBOLD}

                    -a | --accounts  <${yellow}value${reset}>
                    -z | --zipfile   <${yellow}value${reset}>
                    -n | --name      <${yellow}value${reset}>
                   [-r | --region    <${yellow}value${reset}> ]
                   [-s | --skip  ]

  ${title}OPTIONS${bodytext}

        ${BOLD}-a, --accounts${UNBOLD} <value> :  Either (1) a single profile name
            from the local awscli config, or a file containing one
            or more account roles present in local awscli config.

        ${BOLD}-p, --name${UNBOLD} <value> :  Name of AWS Lambda function you wish
            to deploy.  Must match name in AWS account if updating
            a function that pre-exists

        ${BOLD}-z, --zipfile${UNBOLD} <value> :  Deployment package to upload to
            AWS Lambda

        ${BOLD}-r, --region${UNBOLD} <value> : AWS region code (e.g. us-east-1)

        ${BOLD}-s, --skip${UNBOLD} <value> : Skip generation of temp credentials
  ______________________________________________________________________

        -  if --region not provided, default AWS region assumed

        -  Temporary Credentials:  Prepends profilenames auto-
           matically with "gcreds-"
  _____________________________________________________________________

EOM
    #
    # <-- end function put_rule_help -->
}

parse_parameters(){
    if [[ ! "$*" ]]; then
        help_menu
        exit 0
    else
        while [ $# -gt 0 ]; do
            case $1 in
                -a | --accounts)
                    if [ -e "$2" ]; then ACCTFILE=$2; else SINGLEACCT="$2"; fi
                    shift 2
                    ;;
                -n | --name)
                    FUNCTION_NAME=$2
                    shift 2
                    ;;
                -h | --help)
                    put_rule_help
                    shift 1
                    exit 0
                    ;;
                -r | --region)
                    REGION=$2
                    shift 2
                    ;;
                -s | --skip)
                    SKIP="True"
                    shift 1
                    ;;
                -z | --zipfile)
                    ZIPFILE=$2
                    shift 2
                    ;;
                *)
                    echo "unknown parameter. Exiting"
                    exit $E_BADARG
                    ;;
            esac
        done
    fi
    #
    # <-- end function parse_parameters -->
}

function profilename_prefix(){
    local profilename="$1"
    if [ "$(grep "gcreds-$profilename" ~/.aws/credentials)" ]; then
        PROFILE="gcreds-$profilename"
    else
        PROFILE="$profilename"
    fi
    return 0
}

function scan_lambda_names(){
    ##
    ##  Return name of cis baseline function if not set
    ##    - looks for lambda name with 'securitybaseline'
    ##
    echo "$(aws lambda list-functions \
        --profile $PROFILE \
        --region $REGION | jq -r .Functions[].FunctionName | grep -i $LAMBDA_NAME_KEYWORD)"
}

#
# MAIN  ------------------------------------------------------------------
#

parse_parameters $@

if [ ! $SKIP ]; then
    # generate required temporary credentials for acct access
    localgcreds=$(which gcreds)
    $localgcreds -p $IAM_PROFILE -a $ACCTFILE -c $MFACODE
    exitcode=$?
    if [ $exitcode -ne 0 ]; then
        echo -e "\n${white}${BOLD}$pkg${UNBOLD}${reset} exit, failure to generate temporary credentials. (Code $exitcode)\n" | indent04
        exit $exitcode
    fi
fi

if [ "$SINGLEACCT" ]; then
    ARR_ACCOUNTS=( "$SINGLEACCT" )
else
    ARR_ACCOUNTS="$(cat $ACCTFILE)"
fi

# update code, loop thru accounts
for acct in ${ARR_ACCOUNTS[@]}; do
    # determine profile prefix
    profilename_prefix "$acct"

    if [ ! $FUNCTION_NAME ]; then
        FUNCTION_NAME=$(scan_lambda_names)
    fi

    # update code
    echo -e "\nUpdating Code for Function : ${BOLD}${white}$FNCN${UNBOLD}${reset}" | indent10
    echo -e "\nAccount: ${BOLD}${white}$acct${UNBOLD}${reset}" | indent02
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file fileb://$ZIPFILE \
        --profile $PROFILE \
        --region $REGION | jq .

    # determine if aliases
    response=$(aws lambda list-aliases \
        --function-name $FUNCTION_NAME \
        --profile $PROFILE \
        --region $REGION 2>/dev/null)

    if [[ -z "$response" ]]; then
        echo -e "\nNo function alias found.\n"

    elif [[ $(echo $response | grep "Aliases") ]]; then

        # determine alias name
        ALIAS=$(echo $response | jq -r .Aliases[0].Name)

        if [[ "$ALIAS" != null ]]; then

            # ALIAS is not null, point alias to LATEST
            echo -e "\nUpdating Alias ($ALIAS) for Function : ${BOLD}${white}$FUNCTION_NAME${UNBOLD}${reset}" | indent10
            echo -e "\nAccount: ${BOLD}${white}$acct${UNBOLD}${reset}" | indent02

            aws lambda update-alias \
                --function-name "$FUNCTION_NAME" \
                --name "$ALIAS" \
                --function-version "\$LATEST" \
                --profile $PROFILE \
                --region $REGION | jq .

            # validate changes
            echo -e "\nAlias pointer info for alias : ${BOLD}${white}$ALIAS${UNBOLD}${reset}" | indent10
            echo -e "\nAccount: ${BOLD}${white}$acct${UNBOLD}${reset}" | indent02

            aws lambda list-aliases \
                --function-name "$FUNCTION_NAME" \
                --profile $PROFILE \
                --region $REGION | jq .

            echo -e "\n"
        fi
    fi
done

# <-- end -->
exit 0
