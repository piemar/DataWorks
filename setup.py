"""
Setup script for Volvo Service Orders migration project
"""
import os
import sys
import subprocess
from pathlib import Path

def install_requirements():
    """Install required Python packages"""
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        return False

def create_env_file():
    """Create .env file from template"""
    env_file = Path(".env")
    env_example = Path("config.env.example")
    
    if env_file.exists():
        print("✅ .env file already exists")
        return True
    
    if not env_example.exists():
        print("❌ config.env.example not found")
        return False
    
    try:
        # Copy example to .env
        with open(env_example, 'r') as src, open(env_file, 'w') as dst:
            dst.write(src.read())
        
        print("✅ Created .env file from template")
        print("⚠️  Please update .env file with your actual connection strings")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = ["logs", "data", "checkpoints"]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✅ Created directory: {directory}")

def main():
    """Main setup function"""
    print("🚀 Setting up Volvo Service Orders Migration Project")
    print("=" * 50)
    
    # Install requirements
    if not install_requirements():
        sys.exit(1)
    
    # Create .env file
    if not create_env_file():
        sys.exit(1)
    
    # Create directories
    create_directories()
    
    print("\n✅ Setup completed successfully!")
    print("\nNext steps:")
    print("1. Update .env file with your connection strings")
    print("2. Test connections: python test_connection.py")
    print("3. Run: python data_generator.py (to generate test data)")
    print("4. Run: python migrate_to_atlas.py (to migrate to Atlas)")
    print("\nNote: Using MongoDB driver version 3.12.3 for compatibility with older Cosmos DB versions")

if __name__ == "__main__":
    main()
