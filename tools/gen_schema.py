import json

from dispatcherd.config import setup
from dispatcherd.factories import generate_settings_schema

setup(file_path='dispatcher.yml')

data = generate_settings_schema()

print(json.dumps(data, indent=2))
