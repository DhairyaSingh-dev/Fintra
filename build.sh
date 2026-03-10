#!/usr/bin/env bash
# exit on error
set -o errexit

echo "🚀 Starting Fintra build process..."

# Install dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Note: Knowledge base indexing should be done AFTER deployment
# when Redis is actually available. Uncomment below only if Redis 
# is already running during build (rare case):
#
echo "📚 Indexing knowledge base..."
python scripts/index_knowledge.py 

echo "✅ Build complete!"