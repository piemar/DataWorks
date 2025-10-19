#!/bin/bash

# Volvo Service Orders Migration - Quick Start Script
# This script helps you get started quickly with the migration process

set -e

echo "🚀 Volvo Service Orders Migration - Quick Start"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check if Python is installed
check_python() {
    print_header "Checking Python installation..."
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
        print_status "Python3 found: $(python3 --version)"
    elif command -v python &> /dev/null; then
        PYTHON_CMD="python"
        print_status "Python found: $(python --version)"
    else
        print_error "Python is not installed. Please install Python 3.8 or higher."
        exit 1
    fi
}

# Check if pip is installed
check_pip() {
    print_header "Checking pip installation..."
    if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
        print_error "pip is not installed. Please install pip."
        exit 1
    fi
    print_status "pip is available"
}

# Install dependencies
install_dependencies() {
    print_header "Installing dependencies..."
    if [ -f "requirements.txt" ]; then
        $PYTHON_CMD -m pip install -r requirements.txt
        print_status "Dependencies installed successfully"
    else
        print_error "requirements.txt not found"
        exit 1
    fi
}

# Setup environment
setup_environment() {
    print_header "Setting up environment..."
    
    if [ ! -f ".env" ]; then
        if [ -f "config.env.example" ]; then
            cp config.env.example .env
            print_status "Created .env file from template"
            print_warning "Please update .env file with your actual connection strings"
        else
            print_error "config.env.example not found"
            exit 1
        fi
    else
        print_status ".env file already exists"
    fi
    
    # Create necessary directories
    mkdir -p logs data checkpoints
    print_status "Created necessary directories"
}

# Check environment configuration
check_environment() {
    print_header "Checking environment configuration..."
    
    if [ ! -f ".env" ]; then
        print_error ".env file not found. Please run setup first."
        exit 1
    fi
    
    # Source the .env file to check variables
    source .env
    
    if [ -z "$COSMOS_DB_CONNECTION_STRING" ]; then
        print_error "COSMOS_DB_CONNECTION_STRING not set in .env file"
        exit 1
    fi
    
    if [ -z "$MONGODB_ATLAS_CONNECTION_STRING" ]; then
        print_error "MONGODB_ATLAS_CONNECTION_STRING not set in .env file"
        exit 1
    fi
    
    print_status "Environment configuration looks good"
}

# Test database connections
test_connections() {
    print_header "Testing database connections..."
    
    $PYTHON_CMD -c "
import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

async def test_connections():
    try:
        # Test Cosmos DB
        cosmos_client = AsyncIOMotorClient(os.getenv('COSMOS_DB_CONNECTION_STRING'))
        await cosmos_client.admin.command('ping')
        print('✅ Cosmos DB connection successful')
        cosmos_client.close()
        
        # Test Atlas
        atlas_client = AsyncIOMotorClient(os.getenv('MONGODB_ATLAS_CONNECTION_STRING'))
        await atlas_client.admin.command('ping')
        print('✅ MongoDB Atlas connection successful')
        atlas_client.close()
        
    except Exception as e:
        print(f'❌ Connection test failed: {e}')
        exit(1)

asyncio.run(test_connections())
"
    
    if [ $? -eq 0 ]; then
        print_status "Database connections tested successfully"
    else
        print_error "Database connection test failed"
        exit 1
    fi
}

# Show menu
show_menu() {
    echo ""
    echo "What would you like to do?"
    echo "1. Generate test data (1M documents)"
    echo "2. Generate 100GB test data (~10M documents)"
    echo "3. Run performance test"
    echo "4. Start migration"
    echo "5. Monitor migration"
    echo "6. Exit"
    echo ""
    read -p "Enter your choice (1-6): " choice
}

# Generate test data
generate_test_data() {
    local size=$1
    print_header "Generating test data ($size documents)..."
    
    export TOTAL_DOCUMENTS=$size
    export GENERATION_MODE=specific
    
    $PYTHON_CMD data_generator.py
}

# Run performance test
run_performance_test() {
    print_header "Running performance test..."
    $PYTHON_CMD performance_tuner.py
}

# Start migration
start_migration() {
    print_header "Starting migration..."
    print_warning "This will start the migration process. Monitor progress in another terminal."
    read -p "Continue? (y/N): " confirm
    
    if [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]]; then
        $PYTHON_CMD migrate_to_atlas.py
    else
        print_status "Migration cancelled"
    fi
}

# Monitor migration
monitor_migration() {
    print_header "Starting migration monitor..."
    $PYTHON_CMD monitor_migration.py
}

# Main execution
main() {
    # Initial setup
    check_python
    check_pip
    install_dependencies
    setup_environment
    
    # Check if we should skip interactive setup
    if [ "$1" = "--skip-setup" ]; then
        print_status "Skipping interactive setup"
        return
    fi
    
    # Interactive setup
    check_environment
    test_connections
    
    print_status "Setup completed successfully!"
    echo ""
    
    # Main menu loop
    while true; do
        show_menu
        
        case $choice in
            1)
                generate_test_data 1000000
                ;;
            2)
                generate_test_data 10000000
                ;;
            3)
                run_performance_test
                ;;
            4)
                start_migration
                ;;
            5)
                monitor_migration
                ;;
            6)
                print_status "Goodbye!"
                exit 0
                ;;
            *)
                print_error "Invalid choice. Please enter 1-6."
                ;;
        esac
        
        echo ""
        read -p "Press Enter to continue..."
    done
}

# Run main function
main "$@"
