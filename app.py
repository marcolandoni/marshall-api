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
from models.transients.models_transients_put import models_transients_element_put
from models.transients_comments.models_transients_comments import models_transients_comments_put 
from models.transients.models_transients_count import models_transients_count

import traceback

logging.basicConfig(filename='/home/marshall/.config/marshall_api/marshall_api.log', level=logging.INFO)
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
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=24)
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

# GENERIC APP ROUTE FOR GETTING TRANSIENTS DATA

@app.route("/getTransients", methods=["GET"])
@jwt_required()
def getTransients():
  try:
    print(request.json)
    print(dict(request.json))
    print('data1' in dict(request.json))
    model = models_transients_get(log, request.json, db=dbConn, search=True)
    qs, transientData, transientAkas, transientLightcurveData, transientAtelMatches, transients_comments, totalTicketCount, transient_history, transient_crossmatches, skyTags = model.get()
    response ={
      "qs": qs,
      "transientData": transientData,
      "akas": transientAkas,
      "lc_data": transientLightcurveData,
      "ts_atel_matches": transientAtelMatches,
      "comments": transients_comments,
      "totalTicketCount": totalTicketCount,
      "transient_history": transient_history,
      "ts_xmatches": transient_crossmatches,
      "ts_skytag": skyTags

    }
    print("returning ", str(len(transientData)))
    return jsonify(response), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Internal Server Error"}), 505


# GENERIC APP ROUTE FOR PATCHING TRANSIENTS DATA (POSSIBLY CLASSIFICATION WILL GO UNDER THIS ROUTE)
@app.route("/patchTransient", methods=["PATCH"])
@jwt_required()
def patchTransient():
  try:
    request_json = request.json
    #adding the auth user to the request.
    request_json["authenticated_userid"] = get_jwt_identity()
    model = models_transients_element_put(log, request.json, dbConn)
    response = model.put()
    return jsonify({"msg": response}), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Internal Server Error. Please check the format of your request."}), 505

@app.route("/putComment", methods=["PUT"])
@jwt_required()
def putComment():
  try:
    request_json = request.json
    #adding the auth user to the request.
    request_json["authenticated_userid"] = get_jwt_identity()
    model = models_transients_comments_put(log, request.json, dbConn)
    response = model.put()
    return jsonify({"msg": response}), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Internal Server Error. Please check the format of your request."}), 505

@app.route("/countTransients", methods=["GET"])
@jwt_required()
def countTransients():
  try:
    model = models_transients_count(log, request.json, db=dbConn)
    count = model.get()
    return jsonify(count=count), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Internal Server Error"}), 505

if __name__ == "__main__":  
    app.run(port=8000)