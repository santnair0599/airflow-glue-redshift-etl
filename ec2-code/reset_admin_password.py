from airflow import settings
from flask_appbuilder.security.sqla.models import User
from werkzeug.security import generate_password_hash

session = settings.Session()

user = session.query(User).filter_by(username='admin').first()
if user:
    user.password = generate_password_hash('santosh123', method='pbkdf2:sha256')
    session.commit()
    print("✅ Password updated to 'santosh123'")
else:
    print("❌ Admin user not found.")

