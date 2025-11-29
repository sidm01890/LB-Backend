"""
Setup script for FastAPI Reconcii Admin Backend
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return False


def create_env_file():
    """Create .env file from example"""
    env_file = Path(".env")
    env_example = Path("env.example")
    
    if not env_file.exists() and env_example.exists():
        print("🔄 Creating .env file from example...")
        shutil.copy(env_example, env_file)
        print("✅ .env file created. Please update with your configuration.")
        return True
    elif env_file.exists():
        print("✅ .env file already exists")
        return True
    else:
        print("❌ env.example file not found")
        return False


def install_dependencies():
    """Install Python dependencies"""
    return run_command("pip install -r requirements.txt", "Installing Python dependencies")


def setup_database():
    """Setup database (placeholder for now)"""
    print("📝 Database setup instructions:")
    print("1. Make sure MySQL is running")
    print("2. Create databases: 'bercos_sso' and 'devyani'")
    print("3. Update .env file with your database credentials")
    print("4. Run database migrations when available")
    return True


def main():
    """Main setup function"""
    print("🚀 Setting up FastAPI Reconcii Admin Backend...")
    print("=" * 50)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        sys.exit(1)
    
    print(f"✅ Python {sys.version.split()[0]} detected")
    
    # Create virtual environment if it doesn't exist
    venv_path = Path("venv")
    if not venv_path.exists():
        print("🔄 Creating virtual environment...")
        if not run_command("python -m venv venv", "Creating virtual environment"):
            sys.exit(1)
    
    # Activate virtual environment and install dependencies
    if os.name == 'nt':  # Windows
        activate_cmd = "venv\\Scripts\\activate"
        pip_cmd = "venv\\Scripts\\pip"
    else:  # Unix/Linux/macOS
        activate_cmd = "source venv/bin/activate"
        pip_cmd = "venv/bin/pip"
    
    # Install dependencies
    if not run_command(f"{pip_cmd} install -r requirements.txt", "Installing dependencies"):
        sys.exit(1)
    
    # Create .env file
    create_env_file()
    
    # Setup database instructions
    setup_database()
    
    print("=" * 50)
    print("🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Update .env file with your configuration")
    print("2. Setup your databases")
    print("3. Run: python run.py")
    print("4. Visit: http://localhost:8034/api-docs")


if __name__ == "__main__":
    main()
