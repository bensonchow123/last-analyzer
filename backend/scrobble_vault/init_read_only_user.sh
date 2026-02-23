#!/bin/bash

set -e # this makes the script exit on error
echo "Began DB read only user creation..."

READ_ONLY_USER="$POSTGRES_READ_ONLY_USER"
READ_ONLY_PASS="$POSTGRES_READ_ONLY_USER_PASSWORD"

# Uses the default postgres super user to run the init SQL
# The SQL is stored in a bash here document
# -v ON_ERROR_STOP stops the SQL on error
psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_SUPER_USER" \
    --dbname "$POSTGRES_DB" <<-EOSQL

    -- Create the read only user
    CREATE USER "$READ_ONLY_USER" WITH PASSWORD '$READ_ONLY_PASS';

    -- Allow connecting to database
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO "$READ_ONLY_USER";

    -- The schema name should be the default public, no point changing it
    GRANT USAGE ON SCHEMA public TO "$READ_ONLY_USER";

    -- Read access to all existing tables
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO "$READ_ONLY_USER";

    -- Make future tables readable
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO "$READ_ONLY_USER";
EOSQL

# if the exit status is 0 then it is success
if [ $? -eq 0 ]; then
    echo "Read only user initalised."
else
    echo "Read only user initalisation failed."
fi