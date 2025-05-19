#!/bin/bash

# Ensure the init-db.js script has the right permissions
chmod 644 infrastructure/mongodb/init-db.js

# Make sure line endings are correct (especially important if you're using Windows)
sed -i 's/\r$//' infrastructure/mongodb/init-db.js

echo "MongoDB initialization script set up successfully"
EOL

chmod +x infrastructure/mongodb/setup-init-script.sh