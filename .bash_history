sudo apt install python3.10-venv
aws ec2 describe-addresses --region us-east-1
aws configure
sudo apt install awscli
sudo apt update
sudo apt install python3-pip
sudo apt instal python3.10-venv
sudo apt install python3.10-venv
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10 python3.10-venv python3.10-dev
sudo apt install python3.10-venv
python3 -m venv customer_churn_youtube_venv
sudo apt update
sudo apt install python3.12-venv
python3 -m venv customer_churn_youtube_venv
source customer_churn_youtube_venv/bin/activate
sudo pip install apache-airflow
pip install apache-airflow
pip install apache-airflow-providers-amazon
sudo apt update
sudo apt install -y libxml2-dev libxmlsec1-dev libxmlsec1-openssl pkg-config
pip install apache-airflow-providers-amazon
airflow standalone
ls
cd youtube_learning
ls
sudo nano /etc/systemd/system/airflow-webserver.service
sudo nano /etc/systemd/system/airflow-scheduler.service
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver
sudo systemctl enable airflow-scheduler
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler
sudo systemctl status airflow-webserver
ps aux | grep airflow
kill -9 <PID>
kill -9 2244 2246 2249 2250 2251 2252
rm -f /home/ubuntu/airflow/airflow-webserver.pid
systemctl start airflow-webserver
sudo systemctl start airflow-webserver
sudo systemctl status airflow-webserver
source ~/customer_churn_youtube_venv/bin/activate
airflow webserver --port 8080
kill -9 3371
rm -f ~/airflow/airflow-webserver.pid
airflow webserver --port 8080
sudo lsof -i :8080
sudo netstat -tulnp | grep 8080
sudo ss -tulnp | grep 8080
rm -f ~/airflow/airflow-webserver.pid
ps aux | grep airflow
airflow users list
airflow users create   --username admin   --firstname Admin   --lastname User   --role Admin   --email admin@example.com   --password santosh123
airflow shell
from airflow.www.security import get_user_model
from flask_appbuilder.security.manager import SecurityManager
user_model = get_user_model()
sm = SecurityManager()
user = user_model.query.filter_by(username='admin').first()
user.password = sm.get_password_hash('santosh123')
from airflow import settings
session = settings.Session()
session.add(user)
session.commit() exit()
airflow shell
airflow db shell
sudo apt update
sudo apt install sqlite3 -y
airflow db shell
nano reset_admin_password.py
python reset_admin_password.py
nano reset_admin_password.py
python reset_admin_password.py
nano reset_admin_password.py
python reset_admin_password.py
nano reset_admin_password.py
python reset_admin_password.py
python3 test_etl.py
cd ..
ls
rm youtube_learning
rm -rf youtube_learning
ls
mkdir youtube_learning
ls
cd youtube_learning
touch test_etl.py
ls
nano test_etl.py
cd ..
echo $AIRFLOW_HOME
~/airflow
ls
cd airflow
ls
nano airflow.cfg
ls
cd ..
source customer_churn_youtube_venv/bin/activate
ls
airflow webserver --port 8080
airflow standalone
nano remove_example_dags.py
python remove_example_dags.py
nano remove_example_dags.py
python remove_example_dags.py
nano remove_example_dags.py
python remove_example_dags.py
airflow webserver --port 8080
pkill -f "airflow webserver"
airflow webserver --port 8080 &
grep dags_folder ~/airflow/airflow.cfg
ls /home/ubuntu/airflow/dags/example_*.py
airflow dags list | grep example_
airflow dags list
grep load_examples ~/airflow/airflow.cfg
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
airflow db reset
airflow db init
airflow users create   --username admin   --firstname Admin   --lastname User   --role Admin   --email admin@example.com   --password santosh123
airflow scheduler &
airflow webserver --port 8080 &
pkill -f "airflow webserver"
rm -f ~/airflow/airflow-webserver.pid
airflow webserver --port 8080 &
pkill -f "airflow webserver"
rm -f ~/airflow/airflow-webserver.pid
sudo fuser 8080/tcp
sudo kill -9 9397 9400 9401 9402 9403
sudo fuser 8080/tcp
airflow webserver --port 8080 &
rm -f /home/ubuntu/airflow/airflow-webserver.pid
airflow webserver --port 8080 &
ls
ls
airflow webserver --port 8080
airflow db init
source ~/customer_churn_youtube_venv/bin/activate
airflow version
airflow db init
airflow webserver --port 8080
ps aux | grep airflow
airflow dags list
airflow dags list-import-errors
ls -l /home/ubuntu/airflow/dags/customer_churn_dag.py
airflow dags list-import-errors
source ~/customer_churn_youtube_venv/bin/activate
airflow version
airflow db init
airflow webserver --8800
airflow webserver --port 8080
ps aux | grep airflow
airflow webserver --port 8080
airflow dags list-import-errors
ls
ls -l /home/ubuntu/airflow/dags/customer_churn_dag.py
ps aux | grep airflow
airflow dags list-import-errors
nano /home/ubuntu/airflow/dags/customer_churn_dag.py
source customer_churn_youtube_venv/bin/activate
airflow webserver --port 8080
ps aux | grep airflow
airflow webserver --port 8080
curl curl http://localhost:8080/api/v1/health
auth_backend = airflow.api.auth.backend.default
curl http://localhost:8080/api/v1/health
ssh -i customerchurnkeypair.pem -L 8080:localhost:8080 ubuntu@13.218.141.73
ssh -i "customerchurnkeypair.pem" -L 8080:localhost:8080 ubuntu@13.218.141.73
