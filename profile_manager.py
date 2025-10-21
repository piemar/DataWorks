#!/usr/bin/env python3
"""
Profile Management CLI Tool

This tool helps manage and inspect framework profiles.
"""

import sys
import argparse
from pathlib import Path

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent))

from framework.config.profiles import (
    PROFILES, 
    get_profile, 
    list_profiles, 
    show_profile_info, 
    validate_profile_settings
)
from framework.config.manager import ConfigManager

def list_profiles_command():
    """List all available profiles"""
    print("Available Profiles:")
    print("=" * 50)
    
    profiles = list_profiles()
    for name, description in profiles.items():
        profile = get_profile(name)
        use_case = profile.get("use_case", "No description available")
        print(f"\n📋 {name}")
        print(f"   Description: {description}")
        print(f"   Use Case: {use_case}")

def show_profile_command(profile_name: str):
    """Show detailed information about a profile"""
    if profile_name not in PROFILES:
        print(f"❌ Error: Profile '{profile_name}' not found")
        print("\nAvailable profiles:")
        for name in PROFILES.keys():
            print(f"  - {name}")
        return
    
    print(show_profile_info(profile_name))

def validate_command(profile_name: str, settings_file: str = None):
    """Validate profile settings"""
    if profile_name not in PROFILES:
        print(f"❌ Error: Profile '{profile_name}' not found")
        return
    
    # Load settings from file if provided
    user_settings = {}
    if settings_file and Path(settings_file).exists():
        # This would need to be implemented to load settings from file
        print(f"📁 Loading settings from {settings_file}")
        # For now, just show that validation would happen
        print("⚠️  File loading not yet implemented")
    
    # Validate settings
    warnings = validate_profile_settings(profile_name, user_settings)
    
    if warnings:
        print(f"⚠️  Validation warnings for profile '{profile_name}':")
        for key, warning in warnings.items():
            print(f"   {key}: {warning}")
    else:
        print(f"✅ Profile '{profile_name}' settings are valid")

def test_config_command(profile_name: str):
    """Test configuration loading with a profile"""
    print(f"🧪 Testing configuration with profile '{profile_name}'")
    
    try:
        config_manager = ConfigManager()
        config = config_manager.load_config_with_profile(profile_name)
        
        print(f"✅ Configuration loaded successfully")
        print(f"📋 Active profile: {config_manager.get_profile()}")
        
        # Show some key settings
        print(f"\n📊 Key Settings:")
        print(f"   Environment: {config.environment}")
        print(f"   Log Level: {config.log_level}")
        print(f"   Source DB: {config.source_database.database_name}")
        print(f"   Target DB: {config.target_database.database_name}")
        
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")

def create_env_template_command(profile_name: str, output_file: str = ".env_local"):
    """Create a .env_local template for a specific profile"""
    if profile_name not in PROFILES:
        print(f"❌ Error: Profile '{profile_name}' not found")
        return
    
    template = f"""# =============================================================================
# VOLVO DATA FRAMEWORK - {profile_name.upper()} PROFILE CONFIGURATION
# =============================================================================

# Profile Selection
FRAMEWORK_PROFILE={profile_name}

# Database Configuration (REQUIRED)
GEN_DB_CONNECTION_STRING="mongodb://your-account:YOUR_PASSWORD@your-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-account@"
GEN_DB_NAME=volvo-service-orders
GEN_DB_COLLECTION=serviceorders

MIG_TARGET_DB_CONNECTION_STRING="mongodb+srv://your-username:YOUR_PASSWORD@your-cluster.mongodb.net/?retryWrites=false&w=0&appName=YourCluster"
MIG_TARGET_DB_NAME=volvo-service-orders
MIG_TARGET_DB_COLLECTION=serviceorders

# Optional Overrides (uncomment to customize)
# MIG_BATCH_SIZE=15000
# GEN_BATCH_SIZE=40000
"""
    
    with open(output_file, 'w') as f:
        f.write(template)
    
    print(f"✅ Created {output_file} template for profile '{profile_name}'")
    print(f"📝 Edit the file to add your database credentials")

def main():
    parser = argparse.ArgumentParser(
        description="Volvo Data Framework Profile Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python profile_manager.py list
  python profile_manager.py info data-migration
  python profile_manager.py validate data-migration
  python profile_manager.py test data-migration
  python profile_manager.py create data-migration
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List command
    subparsers.add_parser('list', help='List all available profiles')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show detailed profile information')
    info_parser.add_argument('profile', help='Profile name')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate profile settings')
    validate_parser.add_argument('profile', help='Profile name')
    validate_parser.add_argument('--settings-file', help='Settings file to validate')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test configuration loading')
    test_parser.add_argument('profile', help='Profile name')
    
    # Create command
    create_parser = subparsers.add_parser('create', help='Create .env_local template')
    create_parser.add_argument('profile', help='Profile name')
    create_parser.add_argument('--output', default='.env_local', help='Output file name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'list':
        list_profiles_command()
    
    elif args.command == 'info':
        show_profile_command(args.profile)
    
    elif args.command == 'validate':
        validate_command(args.profile, args.settings_file)
    
    elif args.command == 'test':
        test_config_command(args.profile)
    
    elif args.command == 'create':
        create_env_template_command(args.profile, args.output)

if __name__ == '__main__':
    main()
