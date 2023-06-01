from flask import Flask, request
import os

app = Flask(__name__)

USER_PATH = "/Users/emiliano.lara/Documents/Personal/Globant/"
LOCAL_DIR = USER_PATH + "globant_challenge_emiliano_lara/TargetFiles/"
MYSQL_JAR_PATH = USER_PATH + "globant_challenge_emiliano_lara/mysql-connector-j-8.0.32.jar"
SPARK_CODE_PATH = USER_PATH + "globant_challenge_emiliano_lara/spark_loader.py"

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    filename = file.filename
    path = os.path.join(LOCAL_DIR, filename)
    file.save(path)
    table_name = os.path.splitext(filename)[0]
    status = os.system(f'spark-submit --master local[*] --jars {MYSQL_JAR_PATH} {SPARK_CODE_PATH} {path} {table_name}')
    
    result = 'File uploaded successfully' if status == 0 else 'Something went wrong!'
    return result

if __name__ == '__main__':
    app.run(debug=True)