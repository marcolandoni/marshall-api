import logging

from fundamentals.mysql import database, readquery
from packages.login import login_user

from flask import Flask
from flask import jsonify
from flask import request
from flask_jwt_extended import JWTManager
from flask_jwt_extended import jwt_required
from flask_jwt_extended import get_jwt_identity, create_access_token, get_jwt
from datetime import timedelta
import redis
from models.transients.models_transients_get import models_transients_get
import traceback

logging.basicConfig(filename='myapp.log', level=logging.INFO)
log = logging.getLogger(__name__)


dbSettings = {
    'host': '127.0.0.1', 
    'user': 'marshall', 
    'password': 'mar5ha11', 
    'db': 'marshall'
}

dbConn = database(
    log=log,
    dbSettings=dbSettings
).connect()

app = Flask(__name__)
ACCESS_EXPIRES = timedelta(hours=24)
app.config["JWT_SECRET_KEY"] = "super-secret"  # Change this!
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=1)
app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(days=30)
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = ACCESS_EXPIRES


jwt = JWTManager(app)

jwt_redis_blocklist = redis.StrictRedis(
    host="localhost", port=6379, db=0, decode_responses=True
)

@jwt.token_in_blocklist_loader
def check_if_token_is_revoked(jwt_header, jwt_payload: dict):
    jti = jwt_payload["jti"]
    token_in_redis = jwt_redis_blocklist.get(jti)
    return token_in_redis is not None

@app.route("/login", methods=["POST"])
def login():
  try:
    firstname = request.json.get("firstname", None)
    lastname = request.json.get("lastname", None)
    password = request.json.get("password", None)

    return login_user(dbConn, firstname, lastname, password, log)
  except:
    return jsonify({"msg": "Internal Server Error"}), 505


@app.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
  try:
    identity = get_jwt_identity()
    access_token = create_access_token(identity=identity, fresh=False)
    return jsonify(access_token=access_token)
  except:
    return jsonify({"msg": "Internal Server Error"}), 505


@app.route("/logged_user", methods=["GET"])
@jwt_required()
def logged_user():
    # Access the identity of the current user with get_jwt_identity
    try:
      current_user = get_jwt_identity()
      print(current_user)
      return jsonify(logged_in_as=current_user), 200
    except Exception as e:
      return jsonify({"msg": "Internal Server Error"}), 505


@app.route("/logout", methods=["DELETE"])
@jwt_required(verify_type=False)
def logout():
  try:
      token = get_jwt()
      jti = token["jti"]
      ttype = token["type"]
      jwt_redis_blocklist.set(jti, "", ex=ACCESS_EXPIRES)

      # Returns "Access token revoked" or "Refresh token revoked"
      return jsonify(msg=f"{ttype.capitalize()} token successfully revoked")
  except Exception as e:
    return jsonify({"msg": "Internal Server Error"}), 505
    print(e)

@app.route("/getTransients", methods=["GET"])
def getTransients():
  try:
    print(request.json)
    print(dict(request.json))
    print('data1' in dict(request.json))
    model = models_transients_get(log, request.json, db=dbConn, search=True)
    qs, data, akas = model.get()
    response ={
      "transientData": data,
      "akas": akas
    }
    return jsonify(response), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())

if __name__ == "__main__":  
    app.run(port=8000)