import json
from dispatcher.config import setup
from dispatcher.factories import generate_settings_schema

setup(file_path='dispatcher.yml')

data = generate_settings_schema()

print(json.dumps(data, indent=2))
