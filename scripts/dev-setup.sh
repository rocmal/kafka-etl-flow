#!/bin/bash

echo "🔧 Setting up development environment..."

# Copy .env.example to .env
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✓ .env file created"
fi

# Install Node.js dependencies
if [ -d "node-app" ]; then
    echo "Installing Node.js dependencies..."
    cd node-app && npm install && cd ..
    echo "✓ Node.js dependencies installed"
fi

# Install Python dependencies for testing
if [ -f "requirements-dev.txt" ]; then
    echo "Installing Python dev dependencies..."
    pip install -r requirements-dev.txt
    echo "✓ Python dependencies installed"
fi

echo ""
echo "✅ Development environment ready!"
echo ""
echo "Next steps:"
echo "1. docker-compose up -d"
echo "2. ./scripts/init-kafka.sh"
echo "3. npm run producer (in node-app/)"
echo "4. npm run consumer (in node-app/)"
