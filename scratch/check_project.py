import google.auth

try:
    credentials, project_id = google.auth.default()
    print(f"Default Project ID: {project_id}")
except Exception as e:
    print(f"Error getting default project: {e}")
