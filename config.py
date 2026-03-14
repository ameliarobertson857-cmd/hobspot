# config.py: Loads environment variables and exposes them for the project
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Expose HUBSPOT_TOKEN
HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")
