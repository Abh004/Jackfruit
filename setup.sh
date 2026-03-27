#!/bin/bash
# Run this ONCE to scaffold replica1/, replica2/, replica3/
# from the shared ./replica source directory.
#
# After this, each replicaN/ folder is bind-mounted into its container.
# Edit replicaN/main.py → uvicorn auto-reloads that container only.

set -e

for i in 1 2 3; do
  if [ -d "replica$i" ]; then
    echo "replica$i/ already exists — skipping"
  else
    cp -r replica "replica$i"
    echo "Created replica$i/ from ./replica"
  fi
done

echo ""
echo "Done! Your folder structure should look like:"
 echo "./gateway/"
echo "./replica/"
echo "./replica1/"
echo "./replica2/"
echo "./replica3/"
echo "./frontend/"
echo "docker-compose.yml"
