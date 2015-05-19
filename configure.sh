#! /bin/bash

function extractParam {
    PARAM=$(echo "$1" | awk -F= '{ print $2 }')
}

for param in "$@"
do
    case "$param" in 
    --platform=*)
        extractParam "$param"
        PLATFORM="$PARAM"
        ;;
        
    --help)
        cat <<-END
		Usage: ./configure [options]

		Options:
		    --platform={Linux|Darwin}
END
        exit 0
        ;;

    *)
        echo "Unknown parameter: $param"
        exit 1
    esac
done

if [ -z $PLATFORM ]
then
    PLATFORM=$(uname -s)
fi

rm -f platform.mk
case "$PLATFORM" in
Linux)
    echo 'BDB_PLATFORM := linux' >> platform.mk
    ;;

Darwin)
    echo 'BDB_PLATFORM := macosx' >> platform.mk
    ;;
*)
    echo "Platform $PLATFORM is not supported."
    exit 1
esac

echo "HashDB has been successfully configured for $PLATFORM."
