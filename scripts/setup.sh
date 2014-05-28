#!/bin/bash

index="crawler"
type="locations"

#force the creation of normal one (i.e. delete and then create)
echo "Create index if exist delete it waiting for converter.py..."
python ../converter.py index_elastic --index=$index --action force

##create the type and set the mappings
echo "Create the place type waiting for converter.py..."
python ../converter.py type_elastic --index=$index --type=$type

exit;
done
